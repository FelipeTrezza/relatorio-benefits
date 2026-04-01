#!/bin/bash
# Duplo clique neste arquivo para iniciar o servidor do Score Antecipações

echo "🐺 Iniciando servidor Score Antecipações..."
echo ""

# Verificar Python
if ! command -v python3 &>/dev/null; then
  echo "❌ Python3 não encontrado."
  echo "   Instale em: https://www.python.org/downloads/"
  read -p "Pressione Enter para fechar..."
  exit 1
fi

# Instalar databricks-sdk se necessário
if ! python3 -c "import databricks.sdk" 2>/dev/null; then
  echo "📦 Instalando databricks-sdk (só na primeira vez)..."
  pip3 install -q databricks-sdk
fi

# Verificar autenticação Databricks
if ! databricks auth token --profile picpay &>/dev/null 2>&1; then
  echo "🔐 Primeira vez? Fazendo login no Databricks..."
  echo "   (O browser vai abrir para você autorizar)"
  databricks auth login \
    --host https://picpay-principal.cloud.databricks.com \
    --profile picpay
fi

# Criar diretório do servidor se não existir
DEST="$HOME/score-antecipacoes-servidor"
mkdir -p "$DEST"

# Baixar os arquivos do servidor se não existirem
if [ ! -f "$DEST/server.py" ]; then
  echo "📥 Baixando arquivos do servidor..."
  # Copiar do diretório do Felipe se existir, senão criar inline
  if [ -f "$HOME/relatorio-benefits/server.py" ]; then
    cp "$HOME/relatorio-benefits/server.py"              "$DEST/server.py"
    cp "$HOME/relatorio-benefits/funil_query_helper.py"  "$DEST/funil_query_helper.py"
    echo "✅ Arquivos copiados."
  else
    echo "❌ Arquivos do servidor não encontrados."
    echo "   Peça o setup completo para o Felipe Trezza."
    read -p "Pressione Enter para fechar..."
    exit 1
  fi
fi

# Adicionar alias se não existir
SHELL_RC="$HOME/.zshrc"
[ -f "$HOME/.bashrc" ] && SHELL_RC="$HOME/.bashrc"
if ! grep -q "servidor-score" "$SHELL_RC" 2>/dev/null; then
  echo "alias servidor-score='python3 $DEST/server.py'" >> "$SHELL_RC"
fi

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║  ✅ Servidor rodando em localhost:5001   ║"
echo "║  Deixe esta janela aberta enquanto usar  ║"
echo "║  o relatório. Feche para encerrar.       ║"
echo "╚══════════════════════════════════════════╝"
echo ""

python3 "$DEST/server.py"
