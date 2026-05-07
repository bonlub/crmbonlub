"""
CRM Bonlub — Sincronizador automático Symplex/Conttrade → Firebase
Roda a cada 15 minutos, busca NFs novas no PostgreSQL e atualiza o CRM.

Banco: dbCONTTRADE (PostgreSQL)
Tabelas principais:
  saida        — NF de saída (cabeçalho), PK: (saidaeucdg, seriecdg, saidanro)
  saidaitem    — itens da NF
  terceiro     — clientes E vendedores (campo terceirocliente / terceirovendedor)
  produto      — cadastro de produtos
  historicovenda — histórico de vendas por cliente/produto (para carga inicial)
  cidade       — tabela de cidades (cidadenome, cidadeuf)
"""

import json
import logging
import os
import random
import re
import time
from collections import defaultdict
from datetime import datetime

import psycopg2
import psycopg2.extras
import schedule
import firebase_admin
from firebase_admin import credentials, firestore

# ── Configuração ──────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(BASE_DIR, 'config.json'), encoding='utf-8') as f:
    CFG = json.load(f)

SQL = CFG['sql']
COL_CIDADE = CFG.get('colunas_cidade', {'nome': 'cidadenome', 'uf': 'cidadeuf'})

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
    """Fuzzy match de nomes de vendedores (espelho do nomeVendedor() do CRM)."""
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

def nf_id(saidanro, seriecdg):
    """Identificador legível da NF: '12345/NF001'."""
    return f"{saidanro}/{seriecdg}".strip()

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

def conectar_sql():
    """Conecta no PostgreSQL do Symplex/Conttrade (dbCONTTRADE)."""
    return psycopg2.connect(
        host=SQL.get('host', '127.0.0.1'),
        port=int(SQL.get('porta', 5432)),
        dbname=SQL['database'],
        user=SQL['user'],
        password=SQL['password'],
        connect_timeout=15,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )

# ── Consultas SQL (colunas reais do dbCONTTRADE) ──────────────────────────────

def buscar_nfs_novas(conn, desde):
    """
    NFs de saída emitidas após 'desde', não canceladas.
    - saida: cabeçalho da NF (dados do cliente já desnormalizados)
    - terceiro t: dados do cliente (whatsapp)
    - terceiro tv: nome do vendedor (vendedorcdg → terceiro)
    - cidade: nome e UF da cidade do cliente
    """
    col_nome = COL_CIDADE['nome']
    col_uf   = COL_CIDADE['uf']
    sql = f"""
        SELECT
            s.saidaeucdg,
            s.seriecdg,
            s.saidanro,
            s.saidanro::text || '/' || s.seriecdg       AS nf_numero,
            s.saidaemissao                               AS nf_data,
            s.saidatotal                                 AS nf_valor_total,
            s.terceiroeucdg                              AS cli_eucdg,
            s.terceirocdg                                AS cli_cdg,
            COALESCE(s.terceironome, '')                 AS cli_razao,
            COALESCE(s.terceirodocumento, '')            AS cli_cnpj,
            COALESCE(t.terceirowhatsapp, '')             AS cli_whatsapp,
            COALESCE(s.terceirotel01, '')                AS cli_tel,
            COALESCE(s.terceirotel02, '')                AS cli_tel2,
            COALESCE(s.terceiroemail, '')                AS cli_email,
            COALESCE(cid.{col_nome}, '')                 AS cli_cidade,
            COALESCE(cid.{col_uf},   '')                 AS cli_estado,
            COALESCE(tv.terceironome, '')                AS vend_nome
        FROM saida s
        INNER JOIN terceiro t   ON s.terceiroeucdg = t.terceiroeucdg
                               AND s.terceirocdg   = t.terceirocdg
        LEFT  JOIN terceiro tv  ON s.vendedoreucdg = tv.terceiroeucdg
                               AND s.vendedorcdg   = tv.terceirocdg
        LEFT  JOIN cidade cid   ON t.cidadecdg     = cid.cidadecdg
        WHERE s.saidaemissao > %s
          AND (s.saidacancelastatus IS NULL OR s.saidacancelastatus = '')
        ORDER BY s.saidaemissao ASC
    """
    cursor = conn.cursor()
    cursor.execute(sql, (desde,))
    return cursor.fetchall()


def buscar_produtos_nf(conn, eucdg, seriecdg, saidanro):
    """
    Itens da NF identificada pela chave composta (eucdg, seriecdg, saidanro).
    produtodescricao e produtounidade já vêm desnormalizados em saidaitem.
    """
    sql = """
        SELECT
            si.produtocdg                                           AS cod_produto,
            COALESCE(si.produtodescricao, p.produtodesc, '')       AS descricao,
            COALESCE(si.produtounidade, p.produtouniestoque, '')   AS unidade,
            si.saidaitemqtd                                         AS quantidade,
            si.saidaitemvalorunit                                   AS valor_unit,
            si.saidaitemtotal                                       AS valor_total
        FROM saidaitem si
        LEFT JOIN produto p ON si.produtoeucdg = p.produtoeucdg
                           AND si.produtocdg   = p.produtocdg
        WHERE si.saidaeucdg = %s
          AND si.seriecdg   = %s
          AND si.saidanro   = %s
    """
    cursor = conn.cursor()
    cursor.execute(sql, (eucdg, seriecdg, saidanro))
    return [
        {
            'codigo':     str(row['cod_produto']  or '').strip(),
            'descricao':  str(row['descricao']    or '').strip(),
            'unidade':    str(row['unidade']       or '').strip(),
            'qtd':        float(row['quantidade']  or 0),
            'valorUnit':  float(row['valor_unit']  or 0),
            'valorTotal': float(row['valor_total'] or 0),
        }
        for row in cursor.fetchall()
    ]


def buscar_historico_cliente(conn, eucdg, cdg):
    """
    Histórico completo de compras de um cliente via historicovenda.
    Usado na primeira vez que o cliente é encontrado para popular
    comprasHistorico com dados anteriores ao início do sync.
    Agrupa por documento (NF), retorna máx. 50 registros mais recentes.
    """
    sql = """
        SELECT
            COALESCE(hv.historicodocumento, '')     AS nf_numero,
            hv.historicoemissao                     AS nf_data,
            hv.historicoquantidade                  AS quantidade,
            hv.historicovalorunitario               AS valor_unit,
            hv.historicovalortotal                  AS valor_total,
            hv.produtocdg::text                     AS cod_produto,
            COALESCE(p.produtodesc, '')             AS descricao,
            COALESCE(p.produtouniestoque, '')       AS unidade
        FROM historicovenda hv
        LEFT JOIN produto p ON hv.produtoeucdg = p.produtoeucdg
                           AND hv.produtocdg   = p.produtocdg
        WHERE hv.terceiroeucdg = %s
          AND hv.terceirocdg   = %s
        ORDER BY hv.historicoemissao ASC
    """
    cursor = conn.cursor()
    cursor.execute(sql, (eucdg, cdg))
    rows = cursor.fetchall()

    # Agrupa por documento NF
    nfs = defaultdict(lambda: {'data': None, 'valor': 0.0, 'produtos': []})
    for row in rows:
        key = str(row['nf_numero']).strip() or '?'
        if nfs[key]['data'] is None:
            nfs[key]['data'] = formatar_data(row['nf_data'])
        nfs[key]['valor'] += float(row['valor_total'] or 0)
        nfs[key]['produtos'].append({
            'codigo':     str(row['cod_produto'] or '').strip(),
            'descricao':  str(row['descricao']   or '').strip(),
            'unidade':    str(row['unidade']      or '').strip(),
            'qtd':        float(row['quantidade'] or 0),
            'valorUnit':  float(row['valor_unit'] or 0),
            'valorTotal': float(row['valor_total'] or 0),
        })

    resultado = [
        {'nf': nf, 'data': info['data'], 'valor': round(info['valor'], 2), 'produtos': info['produtos']}
        for nf, info in nfs.items()
    ]
    return resultado[-50:]  # máx. 50 NFs mais recentes

# ── Lógica de negócio ─────────────────────────────────────────────────────────

def buscar_cliente(clientes, cod_symplex, cnpj, razao_social):
    """
    Mesma lógica do buscarClienteCRM() do CRM:
    1. Código Symplex (terceirocdg)
    2. CNPJ limpo
    3. Razão social normalizada
    """
    cnpj_limpo = limpar_cnpj(cnpj)
    cod_str = str(cod_symplex) if cod_symplex else ''

    if cod_str:
        c = next((x for x in clientes if str(x.get('codCliente', '')) == cod_str), None)
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
            'clienteId':      cliente['id'],
            'tipo':           tipo,
            'razaoSocial':    cliente.get('razaoSocial', ''),
            'cnpj':           limpar_cnpj(cliente.get('cnpj', '')),
            'cidade':         cliente.get('cidade', ''),
            'estado':         cliente.get('estado', ''),
            'nfs':            nf_numero,
            'vendedor':       vendedor,
            'vendedorDiferente': False,
            'divergencias':   [],
            'camposFaltando': [],
        })

# ── Sincronização principal ───────────────────────────────────────────────────

def sincronizar():
    log("──── Iniciando sincronização ────")

    # 1. Carregar estado atual do Firebase
    try:
        snap = DOC_DADOS.get()
        if not snap.exists:
            log_erro("Documento crm/dados não encontrado no Firebase.")
            return
        data_fb = snap.to_dict()
    except Exception as e:
        log_erro("Erro ao carregar Firebase", e)
        return

    clientes = json.loads(data_fb.get('clientes', '[]'))
    deals    = json.loads(data_fb.get('deals',    '[]'))
    config   = json.loads(data_fb.get('config',   '{}'))
    usuarios = json.loads(data_fb.get('usuarios', '[]'))

    nfs_existentes = {d.get('numOrcamento') for d in deals if d.get('etapa') == 'ganho'}

    ultimo_sync = get_ultimo_sync()
    log(f"Buscando NFs posteriores a: {ultimo_sync}")

    # 2. Conectar no PostgreSQL
    try:
        conn = conectar_sql()
    except Exception as e:
        log_erro("Falha ao conectar no PostgreSQL (dbCONTTRADE)", e)
        return

    try:
        notas = buscar_nfs_novas(conn, ultimo_sync)
    except Exception as e:
        log_erro("Erro na consulta SQL de NFs (tabela saida)", e)
        conn.close()
        return

    log(f"{len(notas)} NF(s) encontrada(s) para processar")

    criados     = 0
    atualizados = 0
    ignoradas   = 0

    for nota in notas:
        try:
            eucdg        = nota['saidaeucdg']
            seriecdg_val = nota['seriecdg']
            saidanro_val = nota['saidanro']
            nf_num       = str(nota['nf_numero']).strip()
            data_nf      = formatar_data(nota['nf_data'])
            valor_total  = float(nota['nf_valor_total'] or 0)
            cli_eucdg    = nota['cli_eucdg']
            cli_cdg      = nota['cli_cdg']
            razao_social = str(nota['cli_razao']    or '').upper().strip()
            cnpj_raw     = str(nota['cli_cnpj']     or '')
            whatsapp     = str(nota['cli_whatsapp'] or '').strip()
            tel          = str(nota['cli_tel']      or '').strip()
            tel2         = str(nota['cli_tel2']     or '').strip()
            email        = str(nota['cli_email']    or '').strip()
            cidade       = str(nota['cli_cidade']   or '').upper().strip()
            estado       = str(nota['cli_estado']   or '').upper().strip()
            nome_vend    = str(nota['vend_nome']     or '').upper().strip()

            if nf_num in nfs_existentes:
                ignoradas += 1
                continue

            # Buscar produtos desta NF
            try:
                produtos = buscar_produtos_nf(conn, eucdg, seriecdg_val, saidanro_val)
            except Exception as e:
                log_erro(f"Erro ao buscar produtos da NF {nf_num}", e)
                produtos = []

            # Resolver vendedor (match fuzzy com usuários cadastrados no CRM)
            usuario_vend   = next((u for u in usuarios if nome_match(u.get('nome', ''), nome_vend)), None)
            vendedor_final = usuario_vend['nome'] if usuario_vend else nome_vend

            # Buscar ou criar cliente
            cliente = buscar_cliente(clientes, cli_cdg, cnpj_raw, razao_social)

            if not cliente:
                # ── Criar novo cliente ──────────────────────────────────────
                cliente = {
                    'id':               uid(),
                    'codCliente':       str(cli_cdg),
                    'razaoSocial':      razao_social,
                    'nomeFantasia':     '',
                    'cnpj':             fmt_cnpj(cnpj_raw),
                    'ie':               '',
                    'whatsapp':         whatsapp,
                    'tel':              tel,
                    'tel2':             tel2,
                    'tel3':             '',
                    'email':            email,
                    'email2':           '',
                    'contato':          '',
                    'cidade':           cidade,
                    'estado':           estado,
                    'transportadora':   '',
                    'categoria':        '',
                    'status':           'CLIENTE ATIVO',
                    'obs':              '',
                    'vendedor':         vendedor_final,
                    'dataVinculo':      hoje() if vendedor_final else '',
                    'cadastradoPor':    'SYMPLEX_AUTO',
                    'global':           True,
                    'reativacoes':      0,
                    'obsHistorico':     [],
                    'valorPotencial':   valor_total,
                    'comprasHistorico': [],
                    'symplexPendente':  True,
                }

                # Carregar histórico completo do cliente (carga inicial)
                if CFG.get('historico_inicial', True):
                    try:
                        historico = buscar_historico_cliente(conn, cli_eucdg, cli_cdg)
                        cliente['comprasHistorico'] = historico
                        log(f"  Histórico: {len(historico)} NF(s) carregadas para {razao_social}")
                    except Exception as e:
                        log_erro(f"Erro ao carregar histórico de {razao_social}", e)

                clientes.append(cliente)
                adicionar_pendencia(config, cliente, 'novo', nf_num, vendedor_final)
                criados += 1
                log(f"  NOVO cliente: {razao_social} | NF {nf_num} | Vendedor: {vendedor_final}")

            else:
                # ── Atualizar cliente existente ─────────────────────────────
                if not cliente.get('codCliente') and cli_cdg:
                    cliente['codCliente'] = str(cli_cdg)
                if not cliente.get('cidade') and cidade:
                    cliente['cidade'] = cidade
                if not cliente.get('estado') and estado:
                    cliente['estado'] = estado
                if not cliente.get('whatsapp') and whatsapp:
                    cliente['whatsapp'] = whatsapp
                if not cliente.get('tel') and tel:
                    cliente['tel'] = tel
                if not cliente.get('email') and email:
                    cliente['email'] = email
                if cliente.get('status') in ('CLIENTE INATIVO', 'LEADS'):
                    cliente['status'] = 'CLIENTE ATIVO'
                if not cliente.get('vendedor') and vendedor_final:
                    cliente['vendedor'] = vendedor_final
                    cliente['dataVinculo'] = hoje()

                adicionar_pendencia(
                    config, cliente, 'atualizado', nf_num,
                    cliente.get('vendedor') or vendedor_final
                )
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
            log_erro(f"Erro ao processar NF {nota.get('nf_numero', '?')}", e)

    conn.close()

    # 3. Salvar no Firebase (somente se houver mudanças)
    if criados + atualizados > 0:
        agora = datetime.now().isoformat()
        try:
            DOC_DADOS.update({
                'clientes':  json.dumps(clientes,  ensure_ascii=False),
                'deals':     json.dumps(deals,      ensure_ascii=False),
                'config':    json.dumps(config,     ensure_ascii=False),
                'updatedAt': agora,
            })
            set_ultimo_sync(agora)
            log(f"Firebase atualizado: {criados} criados, {atualizados} atualizados, {ignoradas} já existiam")
        except Exception as e:
            log_erro("Erro ao salvar no Firebase", e)
    else:
        log(f"Sem alterações. {ignoradas} NF(s) já existiam no CRM.")
        set_ultimo_sync(datetime.now().isoformat())

    log("──── Sincronização concluída ────\n")

# ── Entrada principal ─────────────────────────────────────────────────────────

if __name__ == '__main__':
    intervalo = CFG.get('intervalo_minutos', 15)
    log(f"Serviço CRM Bonlub Sync iniciado — intervalo: {intervalo} min")

    sincronizar()

    schedule.every(intervalo).minutes.do(sincronizar)

    while True:
        schedule.run_pending()
        time.sleep(30)
