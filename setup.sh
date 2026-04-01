#!/bin/bash
# Score Antecipações — Setup + Servidor
# Cole no Terminal: bash <(curl -sL https://felipetrezza.github.io/relatorio-benefits/setup.sh)

set -e
echo ""
echo "╔══════════════════════════════════════════╗"
echo "║  Score Antecipações — Setup do Servidor  ║"
echo "╚══════════════════════════════════════════╝"
echo ""

# ── 1. Python ────────────────────────────────────
if ! command -v python3 &>/dev/null; then
  echo "❌ Python3 não encontrado."
  echo "   Instale em: https://www.python.org/downloads/"
  read -p "Pressione Enter para fechar..."
  exit 1
fi
echo "✅ Python3: $(python3 --version)"

# ── 2. databricks-sdk ────────────────────────────
if ! python3 -c "import databricks.sdk" 2>/dev/null; then
  echo "📦 Instalando databricks-sdk..."
  pip3 install -q databricks-sdk
fi
echo "✅ databricks-sdk OK"

# ── 3. Databricks CLI ────────────────────────────
if ! command -v databricks &>/dev/null; then
  echo "📦 Instalando Databricks CLI..."
  pip3 install -q databricks-cli 2>/dev/null || \
  brew install databricks/tap/databricks 2>/dev/null || true
fi

# ── 4. Autenticação ──────────────────────────────
if ! databricks auth token --profile picpay &>/dev/null 2>&1; then
  echo ""
  echo "🔐 Login Databricks (abrirá o browser)..."
  databricks auth login \
    --host https://picpay-principal.cloud.databricks.com \
    --profile picpay
fi
echo "✅ Databricks autenticado"

# ── 5. Validar acesso às tabelas ─────────────────
echo "🔍 Verificando acesso às tabelas..."
ACESSO=$(python3 -W ignore -c "
import warnings; warnings.filterwarnings('ignore')
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
import time, json

w = WorkspaceClient(host='https://picpay-principal.cloud.databricks.com', profile='picpay')
WH = '6077a99f149e0d70'

tabelas = [
    ('marketing.consumers_campaigns_communications', 'SELECT count(*) FROM marketing.consumers_campaigns_communications LIMIT 1'),
    ('benefits.anticipation_request', 'SELECT count(*) FROM benefits.anticipation_request LIMIT 1'),
    ('benefits.collaborators', 'SELECT count(*) FROM benefits.collaborators LIMIT 1'),
]

erros = []
for nome, sql in tabelas:
    try:
        r = w.statement_execution.execute_statement(warehouse_id=WH, statement=sql, wait_timeout='30s')
        if r.status.state == StatementState.SUCCEEDED:
            print(f'  ✅ {nome}')
        else:
            erros.append(nome)
            print(f'  ❌ {nome}: {r.status.error.message if r.status.error else \"erro desconhecido\"}')
    except Exception as e:
        erros.append(nome)
        print(f'  ❌ {nome}: {e}')

if erros:
    print('ERROS:' + ','.join(erros))
else:
    print('OK')
" 2>/dev/null)

echo "$ACESSO"

if echo "$ACESSO" | grep -q "ERROS:"; then
  TABELAS_ERRO=$(echo "$ACESSO" | grep "ERROS:" | sed 's/ERROS://')
  echo ""
  echo "❌ Sem acesso a algumas tabelas: $TABELAS_ERRO"
  echo "   Solicite acesso em: https://picpedia.picpay.com"
  echo "   Depois rode este script novamente."
  read -p "Pressione Enter para fechar..."
  exit 1
fi
echo "✅ Acesso às tabelas OK"

# ── 6. Baixar arquivos do servidor ───────────────
DEST="$HOME/score-antecipacoes-servidor"
mkdir -p "$DEST"

echo "📥 Baixando arquivos do servidor..."
curl -sL "https://felipetrezza.github.io/relatorio-benefits/server-files/server.py" -o "$DEST/server.py"
curl -sL "https://felipetrezza.github.io/relatorio-benefits/server-files/funil_query_helper.py" -o "$DEST/funil_query_helper.py"

if [ ! -s "$DEST/server.py" ]; then
  echo "❌ Falha ao baixar arquivos."
  exit 1
fi
echo "✅ Arquivos baixados"

# ── 7. Alias ─────────────────────────────────────
for RC in "$HOME/.zshrc" "$HOME/.bashrc"; do
  [ -f "$RC" ] || continue
  if ! grep -q "servidor-benefits" "$RC"; then
    echo "alias servidor-benefits='python3 $DEST/server.py'" >> "$RC"
  fi
done

# ── 8. Iniciar ───────────────────────────────────
echo ""
echo "╔════════════════════════════════════════════════╗"
echo "║  ✅ Tudo certo!                                ║"
echo "║                                                ║"
echo "║  📊 Acesse o relatório em:                    ║"
echo "║  👉 http://localhost:5001/score               ║"
echo "║                                                ║"
echo "║  Deixe esta janela aberta enquanto usar.      ║"
echo "╚════════════════════════════════════════════════╝"
echo ""

python3 "$DEST/server.py"
