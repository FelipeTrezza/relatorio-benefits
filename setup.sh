#!/bin/bash
# Score Antecipações — Setup do Servidor Local
# Cole este comando no Terminal e pressione Enter

set -e
echo ""
echo "🐾 Score Antecipações — Setup do Servidor"
echo "=========================================="

# 1. Verificar Python
if ! command -v python3 &>/dev/null; then
  echo "❌ Python3 não encontrado. Instale em: https://www.python.org/downloads/"
  exit 1
fi
echo "✅ Python3: $(python3 --version)"

# 2. Instalar databricks-sdk se necessário
if ! python3 -c "import databricks.sdk" 2>/dev/null; then
  echo "📦 Instalando databricks-sdk..."
  pip3 install -q databricks-sdk
fi
echo "✅ databricks-sdk OK"

# 3. Configurar autenticação Databricks
if ! databricks auth token --profile picpay &>/dev/null 2>&1; then
  echo "🔐 Abrindo login Databricks no browser..."
  databricks auth login \
    --host https://picpay-principal.cloud.databricks.com \
    --profile picpay
fi
echo "✅ Databricks autenticado"

# 4. Baixar os arquivos do servidor
DEST="$HOME/score-antecipacoes-servidor"
mkdir -p "$DEST"

curl -sL "https://felipetrezza.github.io/relatorio-benefits/server-files/server.py" -o "$DEST/server.py" 2>/dev/null || true
curl -sL "https://felipetrezza.github.io/relatorio-benefits/server-files/funil_query_helper.py" -o "$DEST/funil_query_helper.py" 2>/dev/null || true

# Se download falhou, copiar dos arquivos locais do Felipe
if [ ! -s "$DEST/server.py" ]; then
  if [ -f "$HOME/relatorio-benefits/server.py" ]; then
    cp "$HOME/relatorio-benefits/server.py" "$DEST/server.py"
    cp "$HOME/relatorio-benefits/funil_query_helper.py" "$DEST/funil_query_helper.py"
  else
    echo "❌ Não foi possível baixar os arquivos do servidor."
    echo "   Peça o setup para o Felipe Trezza."
    exit 1
  fi
fi
echo "✅ Arquivos do servidor OK"

# 5. Adicionar alias no shell
for RC in "$HOME/.zshrc" "$HOME/.bashrc"; do
  if [ -f "$RC" ] && ! grep -q "servidor-benefits" "$RC" 2>/dev/null; then
    echo "alias servidor-benefits='python3 $DEST/server.py'" >> "$RC"
    echo "✅ Alias 'servidor-benefits' adicionado em $RC"
  fi
done

echo ""
echo "╔═════════════════════════════════════════╗"
echo "║  ✅ Setup concluído!                    ║"
echo "║                                         ║"
echo "║  Para iniciar o servidor, rode:         ║"
echo "║  python3 ~/score-antecipacoes-servidor/server.py  ║"
echo "║                                         ║"
echo "║  Deixe esta janela aberta enquanto usar ║"
echo "║  o relatório no browser.                ║"
echo "╚═════════════════════════════════════════╝"
echo ""

# 6. Iniciar o servidor automaticamente
python3 "$DEST/server.py"
