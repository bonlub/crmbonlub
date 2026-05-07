"""
Script auxiliar para descobrir os nomes reais das tabelas e colunas
do banco Symplex antes de configurar o config.json.

Execute: python descobrir_tabelas.py
"""

import json, pyodbc, os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(BASE_DIR, 'config.json'), encoding='utf-8') as f:
    CFG = json.load(f)

SQL = CFG['sql']
conn_str = (
    f"DRIVER={{{SQL['driver']}}};"
    f"SERVER={SQL['server']};"
    f"DATABASE={SQL['database']};"
    f"UID={SQL['user']};"
    f"PWD={SQL['password']};"
    "Encrypt=no;TrustServerCertificate=yes;"
)

print("Conectando ao SQL Server...")
conn = pyodbc.connect(conn_str, timeout=10)
cursor = conn.cursor()

# Listar todas as tabelas do banco
print("\n=== TABELAS DISPONÍVEIS ===")
cursor.execute("""
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_NAME
""")
tabelas = [row[0] for row in cursor.fetchall()]
for t in tabelas:
    print(f"  {t}")

# Palavras-chave para identificar as tabelas importantes
keywords = {
    'notas_fiscais': ['nota', 'nf', 'fiscal', 'invoice', 'emissao'],
    'itens_nf':      ['item', 'produto', 'detalhe', 'linha'],
    'clientes':      ['cliente', 'parceiro', 'fornecedor', 'customer'],
    'vendedores':    ['vendedor', 'funcionario', 'representante', 'seller'],
    'produtos':      ['produto', 'item', 'estoque', 'mercadoria'],
}

print("\n=== SUGESTÕES DE MAPEAMENTO ===")
for chave, palavras in keywords.items():
    sugeridas = [t for t in tabelas if any(p in t.lower() for p in palavras)]
    print(f"\n{chave}:")
    if sugeridas:
        for s in sugeridas:
            print(f"  → {s}")
    else:
        print("  (nenhuma encontrada — verifique manualmente)")

# Mostrar colunas das tabelas sugeridas
print("\n=== COLUNAS DAS TABELAS SUGERIDAS ===")
tabelas_ver = set()
for palavras in keywords.values():
    for t in tabelas:
        if any(p in t.lower() for p in palavras):
            tabelas_ver.add(t)

for tabela in sorted(tabelas_ver):
    print(f"\n  [{tabela}]")
    cursor.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{tabela}'
        ORDER BY ORDINAL_POSITION
    """)
    for col in cursor.fetchall():
        tamanho = f"({col[2]})" if col[2] else ''
        print(f"    {col[0]:40} {col[1]}{tamanho}")

conn.close()
print("\n\nConcluído! Use as informações acima para preencher o config.json.")
