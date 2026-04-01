"""
verify_result.py — Verifica se o JSON de resultado está ok
Usado pelo GitHub Actions workflow funil-query.yml
"""
import json, sys, os

rid = os.environ.get("REQUEST_ID", "")
if not rid:
    print("ERRO: REQUEST_ID não definido", file=sys.stderr)
    sys.exit(1)

path = f"results/{rid}.json"
try:
    with open(path) as f:
        d = json.load(f)
except Exception as e:
    print(f"ERRO ao ler {path}: {e}", file=sys.stderr)
    sys.exit(1)

if d.get("ok"):
    print(f"Query OK — {len(d.get('rows', []))} rows | activity: {d.get('activity_name','?')}")
else:
    print(f"Erro na query: {d.get('error', 'desconhecido')}", file=sys.stderr)
    sys.exit(1)
