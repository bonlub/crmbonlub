"""
Script auxiliar para descobrir os nomes reais das tabelas e colunas
do banco Symplex/Conttrade (PostgreSQL) antes de configurar o config.json.

Execute: python descobrir_tabelas.py
"""

import json, os
import psycopg2
import psycopg2.extras

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(BASE_DIR, 'config.json'), encoding='utf-8') as f:
    CFG = json.load(f)

SQL = CFG['sql']

print("Conectando ao PostgreSQL...")
conn = psycopg2.connect(
    host=SQL.get('host', '127.0.0.1'),
    port=int(SQL.get('porta', 5432)),
    dbname=SQL['database'],
    user=SQL['user'],
    password=SQL['password'],
    connect_timeout=10,
)
cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
print(f"Conectado em {SQL.get('host')}:{SQL.get('porta')} → banco: {SQL['database']}\n")

# ── Listar todos os schemas e tabelas ────────────────────────────────────────
print("=== TABELAS DISPONÍVEIS ===")
cursor.execute("""
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
      AND table_schema NOT IN ('pg_catalog', 'information_schema')
    ORDER BY table_schema, table_name
""")
tabelas = cursor.fetchall()
schema_atual = None
for t in tabelas:
    if t['table_schema'] != schema_atual:
        schema_atual = t['table_schema']
        print(f"\n  [{schema_atual}]")
    print(f"    {t['table_name']}")

nomes_tabelas = [t['table_name'] for t in tabelas]

# ── Sugestões automáticas de mapeamento ─────────────────────────────────────
keywords = {
    'notas_fiscais': ['nota', 'nf', 'fiscal', 'invoice', 'emissao', 'emi'],
    'itens_nf':      ['item', 'iten', 'produto', 'detalhe', 'linha', 'nf_prod'],
    'clientes':      ['cliente', 'parceiro', 'fornecedor', 'customer', 'pessoa'],
    'vendedores':    ['vendedor', 'funcionario', 'representante', 'seller', 'vend'],
    'produtos':      ['produto', 'item', 'estoque', 'mercadoria', 'prod'],
}

print("\n\n=== SUGESTÕES DE MAPEAMENTO (atualize o config.json) ===")
for chave, palavras in keywords.items():
    sugeridas = [t for t in nomes_tabelas if any(p in t.lower() for p in palavras)]
    print(f"\n  {chave}:")
    if sugeridas:
        for s in sugeridas:
            print(f"    → {s}")
    else:
        print("    (nenhuma encontrada — verifique manualmente)")

# ── Mostrar colunas das tabelas sugeridas ────────────────────────────────────
tabelas_ver = set()
for palavras in keywords.values():
    for t in nomes_tabelas:
        if any(p in t.lower() for p in palavras):
            tabelas_ver.add(t)

print("\n\n=== COLUNAS DAS TABELAS RELEVANTES ===")
for tabela in sorted(tabelas_ver):
    print(f"\n  [{tabela}]")
    cursor.execute("""
        SELECT column_name, data_type, character_maximum_length, is_nullable
        FROM information_schema.columns
        WHERE table_name = %s
          AND table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY ordinal_position
    """, (tabela,))
    for col in cursor.fetchall():
        tam = f"({col['character_maximum_length']})" if col['character_maximum_length'] else ''
        nulo = '' if col['is_nullable'] == 'YES' else ' NOT NULL'
        print(f"    {col['column_name']:40} {col['data_type']}{tam}{nulo}")

# ── Amostra de dados das tabelas mais relevantes ─────────────────────────────
print("\n\n=== AMOSTRA DE DADOS (5 linhas de cada tabela relevante) ===")
for tabela in sorted(tabelas_ver)[:6]:
    print(f"\n  [{tabela}] — primeiras 5 linhas:")
    try:
        cursor.execute(f'SELECT * FROM "{tabela}" LIMIT 5')
        rows = cursor.fetchall()
        if rows:
            colunas = list(rows[0].keys())
            print(f"    Colunas: {', '.join(colunas)}")
            for row in rows:
                vals = [str(v)[:30] for v in row.values()]
                print(f"    {' | '.join(vals)}")
        else:
            print("    (tabela vazia)")
    except Exception as e:
        print(f"    Erro ao ler: {e}")

conn.close()
print("\n\nConcluído! Copie e cole o resultado acima para mapear o config.json.")
