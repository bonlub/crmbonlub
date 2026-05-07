"""
CRM Bonlub — Sincronizador automático Symplex → Firebase
Roda a cada 15 minutos, busca NFs novas no SQL Server e atualiza o CRM.
"""

import json
import logging
import os
import random
import re
import time
from datetime import datetime

import pyodbc
import schedule
import firebase_admin
from firebase_admin import credentials, firestore

# ── Configuração ──────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(BASE_DIR, 'config.json'), encoding='utf-8') as f:
    CFG = json.load(f)

SQL  = CFG['sql']
TAB  = CFG['tabelas']
COL  = CFG['colunas']

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    filename=os.path.join(BASE_DIR, CFG.get('log_arquivo', 'sync.log')),
    level=logging.INFO,
    format='%(asctime)s %(levelname)s — %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    encoding='utf-8',
)

def log(msg):
    logging.info(msg)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def log_erro(msg, exc=None):
    logging.error(msg, exc_info=exc)
    print(f"[ERRO] {msg}")

# ── Firebase ──────────────────────────────────────────────────────────────────

cred_path = os.path.join(BASE_DIR, CFG['firebase_credentials'])
cred = credentials.Certificate(cred_path)
firebase_admin.initialize_app(cred)
db_firebase = firestore.client()

DOC_DADOS = db_firebase.collection('crm').document('dados')
DOC_META  = db_firebase.collection('crm').document('sync_meta')

# ── Utilitários ───────────────────────────────────────────────────────────────

def uid():
    chars = 'abcdefghijklmnopqrstuvwxyz0123456789'
    return ''.join(random.choices(chars, k=7)) + hex(int(time.time()))[2:]

def hoje():
    return datetime.now().strftime('%Y-%m-%d')

def normalize(s):
    return re.sub(r'[^A-Z0-9]', '', (s or '').upper())

def nome_match(a, b):
    """Fuzzy match de nomes de vendedores (igual ao nomeVendedor() do CRM)."""
    if not a or not b:
        return False
    na, nb = a.upper().strip(), b.upper().strip()
    if na == nb or na.startswith(nb) or nb.startswith(na):
        return True
    partes_a = na.split()
    partes_b = nb.split()
    if partes_a and partes_b:
        return len(partes_a[0]) >= 3 and partes_a[0] == partes_b[0]
    return False

def fmt_cnpj(cnpj_str):
    digits = re.sub(r'\D', '', cnpj_str or '')
    if len(digits) == 14:
        return f"{digits[:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:]}"
    if len(digits) == 11:
        return f"{digits[:3]}.{digits[3:6]}.{digits[6:9]}-{digits[9:]}"
    return digits

def limpar_cnpj(cnpj_str):
    return re.sub(r'\D', '', cnpj_str or '')

def formatar_data(val):
    if val is None:
        return hoje()
    if hasattr(val, 'strftime'):
        return val.strftime('%Y-%m-%d')
    return str(val)[:10]

# ── Controle de última sincronização ─────────────────────────────────────────

def get_ultimo_sync():
    try:
        doc = DOC_META.get()
        if doc.exists:
            return doc.to_dict().get('ultimo_sync', '2000-01-01 00:00:00')
    except Exception as e:
        log_erro("Erro ao ler ultimo_sync", e)
    return '2000-01-01 00:00:00'

def set_ultimo_sync(ts):
    try:
        DOC_META.set({'ultimo_sync': ts, 'ultima_execucao': datetime.now().isoformat()})
    except Exception as e:
        log_erro("Erro ao salvar ultimo_sync", e)

# ── Conexão SQL ───────────────────────────────────────────────────────────────

def _montar_server():
    """Monta a string SERVER do pyodbc a partir de host, porta e instancia."""
    host  = SQL.get('host', SQL.get('server', 'localhost'))
    porta = SQL.get('porta', 1433)
    inst  = (SQL.get('instancia') or '').strip()
    if inst:
        # Instância nomeada: host\instancia  (porta ignorada, SQL Browser resolve)
        return f"{host}\\{inst}"
    # Porta explícita: host,porta
    return f"{host},{porta}"

def conectar_sql():
    conn_str = (
        f"DRIVER={{{SQL.get('driver', 'ODBC Driver 17 for SQL Server')}}};"
        f"SERVER={_montar_server()};"
        f"DATABASE={SQL['database']};"
        f"UID={SQL['user']};"
        f"PWD={SQL['password']};"
        "Encrypt=no;TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, timeout=30)

# ── Consultas SQL (adapte os nomes de tabelas/colunas no config.json) ─────────

def buscar_nfs_novas(conn, desde):
    """
    Retorna todas as NFs emitidas após 'desde'.
    AJUSTE: se o campo de status tiver valores diferentes, edite config.json → colunas.nf_status_emitida
    """
    sql = f"""
        SELECT
            nf.{COL['nf_numero']}        AS nf_numero,
            nf.{COL['nf_data']}          AS nf_data,
            nf.{COL['nf_valor_total']}   AS nf_valor_total,
            c.{COL['cli_codigo']}        AS cli_codigo,
            c.{COL['cli_razao']}         AS cli_razao,
            c.{COL['cli_cnpj']}          AS cli_cnpj,
            c.{COL['cli_cidade']}        AS cli_cidade,
            c.{COL['cli_estado']}        AS cli_estado,
            v.{COL['vend_nome']}         AS vend_nome
        FROM {TAB['notas_fiscais']} nf
        INNER JOIN {TAB['clientes']}   c ON nf.{COL['nf_cod_cliente']}  = c.{COL['cli_codigo']}
        INNER JOIN {TAB['vendedores']} v ON nf.{COL['nf_cod_vendedor']} = v.{COL['vend_codigo']}
        WHERE nf.{COL['nf_data']} > ?
          AND nf.{COL['nf_status']} = '{COL['nf_status_emitida']}'
        ORDER BY nf.{COL['nf_data']} ASC
    """
    cursor = conn.cursor()
    cursor.execute(sql, desde)
    return cursor.fetchall()

def buscar_produtos_nf(conn, nf_numero):
    """Retorna os itens (produtos) de uma NF."""
    sql = f"""
        SELECT
            p.{COL['prod_codigo']}       AS cod_produto,
            p.{COL['prod_descricao']}    AS descricao,
            i.{COL['item_quantidade']}   AS quantidade,
            i.{COL['item_valor_unit']}   AS valor_unit,
            i.{COL['item_valor_total']}  AS valor_total
        FROM {TAB['itens_nf']} i
        INNER JOIN {TAB['produtos']} p ON i.{COL['item_cod_produto']} = p.{COL['prod_codigo']}
        WHERE i.{COL['item_nf_numero']} = ?
    """
    cursor = conn.cursor()
    cursor.execute(sql, nf_numero)
    return [
        {
            'codigo':     str(row.cod_produto or '').strip(),
            'descricao':  str(row.descricao   or '').strip(),
            'qtd':        float(row.quantidade or 0),
            'valorUnit':  float(row.valor_unit or 0),
            'valorTotal': float(row.valor_total or 0),
        }
        for row in cursor.fetchall()
    ]

# ── Lógica de negócio ─────────────────────────────────────────────────────────

def buscar_cliente(clientes, cod_symplex, cnpj, razao_social):
    """Mesma lógica do buscarClienteCRM() do CRM: código → CNPJ → razão social."""
    cnpj_limpo = limpar_cnpj(cnpj)
    if cod_symplex:
        c = next((x for x in clientes if x.get('codCliente') == cod_symplex), None)
        if c:
            return c
    if cnpj_limpo:
        c = next((x for x in clientes if limpar_cnpj(x.get('cnpj', '')) == cnpj_limpo), None)
        if c:
            return c
    rs = normalize(razao_social)
    c = next(
        (x for x in clientes
         if normalize(x.get('razaoSocial', '')) == rs
         or normalize(x.get('razaoSocial', '')).startswith(rs[:15])),
        None
    )
    return c

def adicionar_historico(cliente, nf_numero, data_nf, valor_total, produtos):
    if 'comprasHistorico' not in cliente:
        cliente['comprasHistorico'] = []
    if any(h.get('nf') == nf_numero for h in cliente['comprasHistorico']):
        return
    cliente['comprasHistorico'].append({
        'nf':       nf_numero,
        'data':     data_nf,
        'valor':    valor_total,
        'produtos': produtos,
    })
    if len(cliente['comprasHistorico']) > 50:
        cliente['comprasHistorico'] = cliente['comprasHistorico'][-50:]

def adicionar_pendencia(config, cliente, tipo, nf_numero, vendedor):
    if 'symplexPendencias' not in config:
        config['symplexPendencias'] = []
    existente = next(
        (p for p in config['symplexPendencias'] if p.get('clienteId') == cliente['id']),
        None
    )
    if existente:
        nfs = existente.get('nfs', '')
        if nf_numero not in nfs:
            existente['nfs'] = f"{nfs}, {nf_numero}" if nfs else nf_numero
    else:
        config['symplexPendencias'].append({
            'clienteId':  cliente['id'],
            'tipo':       tipo,
            'razaoSocial': cliente.get('razaoSocial', ''),
            'cnpj':       limpar_cnpj(cliente.get('cnpj', '')),
            'cidade':     cliente.get('cidade', ''),
            'estado':     cliente.get('estado', ''),
            'nfs':        nf_numero,
            'vendedor':   vendedor,
            'vendedorDiferente': False,
            'divergencias':  [],
            'camposFaltando': [],
        })

# ── Sincronização principal ───────────────────────────────────────────────────

def sincronizar():
    log("──── Iniciando sincronização ────")

    # 1. Carregar estado atual do Firebase
    try:
        snap = DOC_DADOS.get()
        if not snap.exists:
            log_erro("Documento crm/dados não encontrado no Firebase. Verifique o projeto.")
            return
        data_fb = snap.to_dict()
    except Exception as e:
        log_erro("Erro ao carregar Firebase", e)
        return

    clientes = json.loads(data_fb.get('clientes', '[]'))
    deals    = json.loads(data_fb.get('deals',    '[]'))
    config   = json.loads(data_fb.get('config',   '{}'))
    usuarios = json.loads(data_fb.get('usuarios', '[]'))

    # Conjunto de NFs já registradas (evita duplicatas)
    nfs_existentes = {d.get('numOrcamento') for d in deals if d.get('etapa') == 'ganho'}

    ultimo_sync = get_ultimo_sync()
    log(f"Buscando NFs posteriores a: {ultimo_sync}")

    # 2. Conectar no SQL Server e buscar NFs novas
    try:
        conn = conectar_sql()
    except Exception as e:
        log_erro("Falha ao conectar no SQL Server", e)
        return

    try:
        notas = buscar_nfs_novas(conn, ultimo_sync)
    except Exception as e:
        log_erro("Erro na consulta SQL de NFs", e)
        conn.close()
        return

    log(f"{len(notas)} NF(s) encontrada(s) para processar")

    criados    = 0
    atualizados = 0
    ignoradas   = 0

    for nota in notas:
        try:
            nf_num       = str(nota.nf_numero).strip()
            data_nf      = formatar_data(nota.nf_data)
            valor_total  = float(nota.nf_valor_total or 0)
            cod_cli      = str(nota.cli_codigo  or '').strip()
            razao_social = str(nota.cli_razao   or '').upper().strip()
            cnpj_raw     = str(nota.cli_cnpj    or '')
            cidade       = str(nota.cli_cidade  or '').upper().strip()
            estado       = str(nota.cli_estado  or '').upper().strip()
            nome_vend    = str(nota.vend_nome   or '').upper().strip()

            # NF já importada?
            if nf_num in nfs_existentes:
                ignoradas += 1
                continue

            # Buscar produtos desta NF
            try:
                produtos = buscar_produtos_nf(conn, nf_num)
            except Exception as e:
                log_erro(f"Erro ao buscar produtos da NF {nf_num}", e)
                produtos = []

            # Resolver vendedor
            usuario_vend  = next((u for u in usuarios if nome_match(u.get('nome', ''), nome_vend)), None)
            vendedor_final = usuario_vend['nome'] if usuario_vend else nome_vend

            # Buscar ou criar cliente
            cliente = buscar_cliente(clientes, cod_cli, cnpj_raw, razao_social)

            if not cliente:
                # ── Criar novo cliente ──────────────────────────────────────
                cliente = {
                    'id':              uid(),
                    'codCliente':      cod_cli,
                    'razaoSocial':     razao_social,
                    'nomeFantasia':    '',
                    'cnpj':            fmt_cnpj(cnpj_raw),
                    'ie':              '',
                    'whatsapp':        '',
                    'tel':             '',
                    'tel2':            '',
                    'tel3':            '',
                    'email':           '',
                    'email2':          '',
                    'contato':         '',
                    'cidade':          cidade,
                    'estado':          estado,
                    'transportadora':  '',
                    'categoria':       '',
                    'status':          'CLIENTE ATIVO',
                    'obs':             '',
                    'vendedor':        vendedor_final,
                    'dataVinculo':     hoje() if vendedor_final else '',
                    'cadastradoPor':   'SYMPLEX_AUTO',
                    'global':          True,
                    'reativacoes':     0,
                    'obsHistorico':    [],
                    'valorPotencial':  valor_total,
                    'comprasHistorico': [],
                    'symplexPendente': True,
                }
                clientes.append(cliente)
                adicionar_pendencia(config, cliente, 'novo', nf_num, vendedor_final)
                criados += 1
                log(f"  NOVO cliente: {razao_social} | NF {nf_num} | Vendedor: {vendedor_final}")

            else:
                # ── Atualizar cliente existente ─────────────────────────────
                if not cliente.get('codCliente') and cod_cli:
                    cliente['codCliente'] = cod_cli
                if not cliente.get('cidade') and cidade:
                    cliente['cidade'] = cidade
                if not cliente.get('estado') and estado:
                    cliente['estado'] = estado
                if cliente.get('status') in ('CLIENTE INATIVO', 'LEADS'):
                    cliente['status'] = 'CLIENTE ATIVO'
                if not cliente.get('vendedor') and vendedor_final:
                    cliente['vendedor'] = vendedor_final
                    cliente['dataVinculo'] = hoje()
                adicionar_pendencia(config, cliente, 'atualizado', nf_num, cliente.get('vendedor') or vendedor_final)
                atualizados += 1
                log(f"  ATUALIZADO: {razao_social} | NF {nf_num}")

            # Histórico de compras
            adicionar_historico(cliente, nf_num, data_nf, valor_total, produtos)

            # Deal de venda efetivada
            deals.append({
                'id':           uid(),
                'clienteId':    cliente['id'],
                'valor':        valor_total,
                'etapa':        'ganho',
                'vendedor':     vendedor_final,
                'data':         data_nf,
                'obs':          'IMPORTADO AUTOMATICAMENTE DO SYMPLEX',
                'motivo':       '',
                'numOrcamento': nf_num,
                'historico': [{
                    'etapa': 'ganho',
                    'data':  data_nf,
                    'obs':   'SYMPLEX AUTO',
                    'autor': 'SISTEMA',
                }],
            })
            nfs_existentes.add(nf_num)

        except Exception as e:
            log_erro(f"Erro ao processar NF {getattr(nota, 'nf_numero', '?')}", e)

    conn.close()

    # 3. Salvar no Firebase (somente se houver mudanças)
    if criados + atualizados > 0:
        agora = datetime.now().isoformat()
        try:
            DOC_DADOS.update({
                'clientes':  json.dumps(clientes,  ensure_ascii=False),
                'deals':     json.dumps(deals,     ensure_ascii=False),
                'config':    json.dumps(config,    ensure_ascii=False),
                'updatedAt': agora,
            })
            set_ultimo_sync(agora)
            log(f"Firebase atualizado: {criados} criados, {atualizados} atualizados, {ignoradas} já existiam")
        except Exception as e:
            log_erro("Erro ao salvar no Firebase", e)
    else:
        log(f"Sem alterações. {ignoradas} NF(s) já existiam no CRM.")
        # Atualizar timestamp mesmo sem mudanças para avançar a janela de busca
        set_ultimo_sync(datetime.now().isoformat())

    log("──── Sincronização concluída ────\n")

# ── Entrada principal ─────────────────────────────────────────────────────────

if __name__ == '__main__':
    intervalo = CFG.get('intervalo_minutos', 15)
    log(f"Serviço CRM Bonlub Sync iniciado — intervalo: {intervalo} min")

    # Primeira execução imediata
    sincronizar()

    # Agendar execuções periódicas
    schedule.every(intervalo).minutes.do(sincronizar)

    while True:
        schedule.run_pending()
        time.sleep(30)
