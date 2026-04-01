"""
get_token.py — Renovar access token Databricks via refresh token OAuth
Usado pelo GitHub Actions workflow funil-query.yml
"""
import urllib.request, urllib.parse, json, os, sys

host          = os.environ.get("DATABRICKS_HOST", "https://picpay-principal.cloud.databricks.com")
refresh_token = os.environ.get("DATABRICKS_REFRESH_TOKEN", "")

if not refresh_token:
    print("ERRO: DATABRICKS_REFRESH_TOKEN nao configurado", file=sys.stderr)
    sys.exit(1)

data = urllib.parse.urlencode({
    "grant_type":    "refresh_token",
    "refresh_token": refresh_token,
    "client_id":     "databricks-cli",
}).encode()

req = urllib.request.Request(
    f"{host}/oidc/v1/token",
    data=data,
    headers={"Content-Type": "application/x-www-form-urlencoded"}
)

try:
    with urllib.request.urlopen(req, timeout=30) as r:
        d = json.loads(r.read())
        token = d.get("access_token", "")
        if not token:
            print("ERRO: access_token vazio na resposta", file=sys.stderr)
            sys.exit(1)
        # Escrever no GITHUB_ENV para o próximo step
        github_env = os.environ.get("GITHUB_ENV", "")
        if github_env:
            with open(github_env, "a") as f:
                f.write(f"DATABRICKS_ACCESS_TOKEN={token}\n")
        print("Token renovado com sucesso")
except Exception as e:
    print(f"ERRO ao renovar token: {e}", file=sys.stderr)
    sys.exit(1)
