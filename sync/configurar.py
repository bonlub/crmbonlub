"""
CRM Bonlub Sync — Configurador interativo
Permite alterar IP, porta, senha e outras configurações sem editar JSON manualmente.
"""

import json
import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config.json')

# ── Cores para terminal Windows ───────────────────────────────────────────────
def cor(texto, codigo):
    return f"\033[{codigo}m{texto}\033[0m"

VERDE    = lambda t: cor(t, '92')
AMARELO  = lambda t: cor(t, '93')
VERMELHO = lambda t: cor(t, '91')
AZUL     = lambda t: cor(t, '94')
NEGRITO  = lambda t: cor(t, '1')

# ── Leitura / escrita do config ───────────────────────────────────────────────
def ler_config():
    with open(CONFIG_PATH, encoding='utf-8') as f:
        return json.load(f)

def salvar_config(cfg):
    with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    print(VERDE("  ✓ Configuração salva com sucesso."))

# ── Utilitários ───────────────────────────────────────────────────────────────
def limpar():
    os.system('cls' if os.name == 'nt' else 'clear')

def pausar():
    input("\nPressione Enter para continuar...")

def ocultar_senha(s):
    if not s or s.startswith('SUA_'):
        return VERMELHO('(não configurada)')
    return '●' * min(len(s), 8)

def status_campo(valor, placeholder):
    if not valor or valor == placeholder:
        return VERMELHO('NÃO CONFIGURADO')
    return VERDE(valor)

def cabecalho():
    limpar()
    print(NEGRITO("═" * 52))
    print(NEGRITO("  CRM Bonlub Sync — Configuração do servidor SQL"))
    print(NEGRITO("═" * 52))

def mostrar_config_atual(cfg):
    sql = cfg['sql']
    porta_str = str(sql.get('porta', 1433))
    inst_str  = sql.get('instancia', '') or '(padrão)'
    print(f"""
  {AZUL('CONEXÃO SQL SERVER')}
  ┌─────────────────────────────────────────────┐
  │  IP do servidor : {status_campo(sql.get('host',''), '192.168.1.100')}
  │  Porta          : {VERDE(porta_str)}
  │  Instância      : {inst_str}
  │  Banco de dados : {status_campo(sql.get('database',''), 'NOME_DO_BANCO_SYMPLEX')}
  │  Usuário        : {status_campo(sql.get('user',''), 'usuario_sql')}
  │  Senha          : {ocultar_senha(sql.get('password',''))}
  └─────────────────────────────────────────────┘
  {AZUL('FIREBASE')}
  ┌─────────────────────────────────────────────┐
  │  Credenciais    : {status_campo(cfg.get('firebase_credentials',''), '')}
  └─────────────────────────────────────────────┘
  {AZUL('SINCRONIZAÇÃO')}
  ┌─────────────────────────────────────────────┐
  │  Intervalo      : {VERDE(str(cfg.get('intervalo_minutos', 15)) + ' minutos')}
  └─────────────────────────────────────────────┘""")

def menu_opcoes():
    print(f"""
  {NEGRITO('O que deseja alterar?')}

    {AMARELO('[1]')} IP do servidor
    {AMARELO('[2]')} Porta
    {AMARELO('[3]')} Instância SQL (opcional)
    {AMARELO('[4]')} Nome do banco de dados
    {AMARELO('[5]')} Usuário do banco
    {AMARELO('[6]')} Senha do banco
    {AMARELO('[7]')} Intervalo de sincronização
    {AMARELO('[T]')} Testar conexão agora
    {AMARELO('[0]')} Sair

  """)

def pedir_valor(label, atual, oculto=False):
    exibir = ocultar_senha(atual) if oculto else (atual or '')
    if exibir:
        prompt = f"  {label} [{exibir}]: "
    else:
        prompt = f"  {label}: "
    novo = input(prompt).strip()
    return novo if novo else atual

# ── Teste de conexão ──────────────────────────────────────────────────────────
def testar_conexao(cfg):
    print(f"\n  {AMARELO('Testando conexão PostgreSQL...')}")
    try:
        import psycopg2
    except ImportError:
        print(VERMELHO("  ✗ psycopg2 não instalado. Execute instalar.bat primeiro."))
        return

    sql = cfg['sql']
    host  = sql.get('host', '127.0.0.1')
    porta = int(sql.get('porta', 5432))

    try:
        conn = psycopg2.connect(
            host=host,
            port=porta,
            dbname=sql.get('database', ''),
            user=sql.get('user', ''),
            password=sql.get('password', ''),
            connect_timeout=8,
        )
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        versao = cursor.fetchone()[0].split(',')[0]
        cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema')")
        n_tabelas = cursor.fetchone()[0]
        conn.close()
        print(VERDE(f"  ✓ Conexão bem-sucedida!"))
        print(f"  Servidor : {versao}")
        print(f"  Tabelas  : {n_tabelas} encontradas no banco")
    except psycopg2.Error as e:
        print(VERMELHO(f"  ✗ Falha na conexão:"))
        print(f"     {e}")
        print(f"\n  Verifique:")
        print(f"    • IP correto: {host}")
        print(f"    • Porta aberta: {porta}  (padrão PostgreSQL)")
        print(f"    • Usuário/senha corretos")
        print(f"    • PostgreSQL permite conexões remotas (pg_hba.conf)")

# ── Menu principal ────────────────────────────────────────────────────────────
def main():
    # Habilitar cores no terminal Windows
    if os.name == 'nt':
        os.system('color')

    while True:
        cfg = ler_config()
        cabecalho()
        mostrar_config_atual(cfg)
        menu_opcoes()

        escolha = input("  Escolha: ").strip().upper()

        if escolha == '0':
            print("\n  Até logo!\n")
            break

        elif escolha == '1':
            cabecalho()
            print(f"\n  {AZUL('IP do servidor SQL Server')}")
            print("  Ex: 192.168.1.100  ou  10.0.0.5  ou  meu-servidor.com\n")
            novo = pedir_valor("Novo IP", cfg['sql'].get('host', ''))
            if novo:
                cfg['sql']['host'] = novo
                salvar_config(cfg)
            pausar()

        elif escolha == '2':
            cabecalho()
            print(f"\n  {AZUL('Porta do SQL Server')}")
            print("  Padrão: 1433  —  altere apenas se o DBA configurou porta diferente\n")
            novo = pedir_valor("Nova porta", str(cfg['sql'].get('porta', 1433)))
            try:
                cfg['sql']['porta'] = int(novo)
                salvar_config(cfg)
            except ValueError:
                print(VERMELHO("  ✗ Porta inválida. Use apenas números."))
            pausar()

        elif escolha == '3':
            cabecalho()
            print(f"\n  {AZUL('Instância SQL Server (opcional)')}")
            print("  Deixe em branco se o servidor usa porta explícita (ex: 1433)")
            print("  Preencha se o Symplex usa instância nomeada (ex: SQLEXPRESS)\n")
            novo = pedir_valor("Instância", cfg['sql'].get('instancia', ''))
            cfg['sql']['instancia'] = novo
            salvar_config(cfg)
            pausar()

        elif escolha == '4':
            cabecalho()
            print(f"\n  {AZUL('Nome do banco de dados')}")
            print("  Execute descobrir_tabelas.py para confirmar o nome correto\n")
            novo = pedir_valor("Banco de dados", cfg['sql'].get('database', ''))
            if novo:
                cfg['sql']['database'] = novo
                salvar_config(cfg)
            pausar()

        elif escolha == '5':
            cabecalho()
            print(f"\n  {AZUL('Usuário do banco de dados')}\n")
            novo = pedir_valor("Usuário", cfg['sql'].get('user', ''))
            if novo:
                cfg['sql']['user'] = novo
                salvar_config(cfg)
            pausar()

        elif escolha == '6':
            cabecalho()
            print(f"\n  {AZUL('Senha do banco de dados')}")
            print("  A senha ficará salva no config.json (não compartilhe este arquivo)\n")
            import getpass
            nova = getpass.getpass("  Nova senha (oculta): ").strip()
            if nova:
                cfg['sql']['password'] = nova
                salvar_config(cfg)
            else:
                print(AMARELO("  Nenhuma alteração feita."))
            pausar()

        elif escolha == '7':
            cabecalho()
            print(f"\n  {AZUL('Intervalo de sincronização')}")
            print("  Mínimo recomendado: 5 minutos  |  Padrão: 15 minutos\n")
            novo = pedir_valor("Intervalo (minutos)", str(cfg.get('intervalo_minutos', 15)))
            try:
                minutos = int(novo)
                if minutos < 1:
                    raise ValueError
                cfg['intervalo_minutos'] = minutos
                salvar_config(cfg)
            except ValueError:
                print(VERMELHO("  ✗ Valor inválido."))
            pausar()

        elif escolha == 'T':
            cabecalho()
            mostrar_config_atual(cfg)
            testar_conexao(cfg)
            pausar()

        else:
            print(VERMELHO("\n  Opção inválida."))
            pausar()

if __name__ == '__main__':
    main()
