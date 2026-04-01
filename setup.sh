#!/bin/bash
# Score Antecipações — Setup + Servidor
# Cole no Terminal: bash <(curl -sL https://felipetrezza.github.io/relatorio-benefits/setup.sh)

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║  Score Antecipações — Setup              ║"
echo "╚══════════════════════════════════════════╝"
echo ""

# ── 1. Python ────────────────────────────────────
if ! command -v python3 &>/dev/null; then
  echo "❌ Python3 não encontrado."
  echo "   Instale em: https://www.python.org/downloads/"
  exit 1
fi
echo "✅ Python3 OK"

# ── 2. pip ───────────────────────────────────────
PIP=""
if command -v pip3 &>/dev/null; then PIP=pip3
elif python3 -m pip --version &>/dev/null 2>&1; then PIP="python3 -m pip"
else
  echo "❌ pip não encontrado. Instale o Python pelo site oficial."
  exit 1
fi

# ── 3. databricks-sdk ────────────────────────────
if ! python3 -c "import databricks.sdk" 2>/dev/null; then
  echo "📦 Instalando databricks-sdk..."
  $PIP install -q databricks-sdk
fi
echo "✅ databricks-sdk OK"

# ── 4. Databricks CLI moderno ────────────────────
if ! command -v databricks &>/dev/null || ! databricks auth --help &>/dev/null 2>&1; then
  echo "📦 Instalando Databricks CLI..."
  if command -v brew &>/dev/null; then
    brew install databricks/tap/databricks 2>/dev/null || \
    brew upgrade databricks 2>/dev/null || true
  else
    # Download direto (sem brew, sem sudo) — instala em ~/.local/bin
    mkdir -p "$HOME/.local/bin"
    ARCH=$(uname -m)
    [ "$ARCH" = "arm64" ] && ARCH="arm64" || ARCH="amd64"
    URL="https://github.com/databricks/cli/releases/latest/download/databricks_cli_darwin_${ARCH}.zip"
    curl -sL "$URL" -o /tmp/databricks_cli.zip
    unzip -q /tmp/databricks_cli.zip -d "$HOME/.local/bin/"
    chmod +x "$HOME/.local/bin/databricks"
    export PATH="$HOME/.local/bin:$PATH"
    echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$HOME/.zshrc" 2>/dev/null || true
    rm /tmp/databricks_cli.zip
  fi
fi

if ! command -v databricks &>/dev/null; then
  echo "❌ Não foi possível instalar o Databricks CLI."
  echo "   Instale manualmente: https://docs.databricks.com/dev-tools/cli/install.html"
  exit 1
fi
echo "✅ Databricks CLI OK: $(databricks --version 2>/dev/null | head -1)"

# ── 5. Login Databricks ───────────────────────────
if ! databricks auth token --profile picpay &>/dev/null 2>&1; then
  echo ""
  echo "🔐 Fazendo login no Databricks..."
  echo "   (O browser vai abrir para você autorizar)"
  echo ""
  databricks auth login \
    --host https://picpay-principal.cloud.databricks.com \
    --profile picpay
fi
echo "✅ Databricks autenticado"

# ── 6. Validar acesso às tabelas ─────────────────
echo ""
echo "🔍 Verificando acesso às tabelas..."
python3 -W ignore << 'PYEOF'
import warnings; warnings.filterwarnings('ignore')
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

w = WorkspaceClient(host='https://picpay-principal.cloud.databricks.com', profile='picpay')
WH = '6077a99f149e0d70'

tabelas = [
    'marketing.consumers_campaigns_communications',
    'benefits.anticipation_request',
    'benefits.collaborators',
    'benefits.companies',
    'benefits.accounts',
]

erros = []
for t in tabelas:
    try:
        r = w.statement_execution.execute_statement(
            warehouse_id=WH,
            statement=f'SELECT 1 FROM {t} LIMIT 1',
            wait_timeout='30s'
        )
        if r.status.state == StatementState.SUCCEEDED:
            print(f'  ✅ {t}')
        else:
            erros.append(t)
            msg = r.status.error.message if r.status.error else 'sem permissão'
            print(f'  ❌ {t}: {msg}')
    except Exception as e:
        erros.append(t)
        print(f'  ❌ {t}: {e}')

if erros:
    print('')
    print('❌ Sem acesso a algumas tabelas.')
    print('   Solicite acesso em: https://picpedia.picpay.com')
    for t in erros:
        schema, tbl = t.split('.')[1], t.split('.')[2]
        print(f'   👉 https://picpedia.picpay.com/glossario-de-negocios/tables/{schema}/{tbl}')
    exit(1)
else:
    print('')
    print('✅ Acesso a todas as tabelas OK!')
PYEOF

# Se a validação falhou, parar aqui
[ $? -eq 0 ] || exit 1

# ── 7. Baixar arquivos do servidor ───────────────
DEST="$HOME/score-antecipacoes-servidor"
mkdir -p "$DEST"

echo ""
echo "📥 Baixando arquivos do servidor..."
curl -sL "https://felipetrezza.github.io/relatorio-benefits/server-files/server.py" -o "$DEST/server.py"
curl -sL "https://felipetrezza.github.io/relatorio-benefits/server-files/funil_query_helper.py" -o "$DEST/funil_query_helper.py"

if [ ! -s "$DEST/server.py" ]; then
  echo "❌ Falha ao baixar arquivos do servidor."
  exit 1
fi
echo "✅ Arquivos baixados"

# ── 8. Alias ─────────────────────────────────────
for RC in "$HOME/.zshrc" "$HOME/.bashrc"; do
  [ -f "$RC" ] || continue
  if ! grep -q "servidor-benefits" "$RC" 2>/dev/null; then
    echo "alias servidor-benefits='python3 $DEST/server.py'" >> "$RC"
  fi
done

# ── 9. Iniciar servidor ──────────────────────────
echo ""
echo "╔════════════════════════════════════════════════╗"
echo "║  ✅ Setup concluído!                           ║"
echo "║                                                ║"
echo "║  📊 Abrindo o relatório em:                   ║"
echo "║  👉 http://localhost:5001/score               ║"
echo "║                                                ║"
echo "║  Deixe esta janela aberta enquanto usar.      ║"
echo "║  Nas próximas vezes: servidor-benefits        ║"
echo "╚════════════════════════════════════════════════╝"
echo ""

# Abrir o browser automaticamente
sleep 1
open "http://localhost:5001/score" 2>/dev/null || \
  xdg-open "http://localhost:5001/score" 2>/dev/null || true

python3 "$DEST/server.py"
