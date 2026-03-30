#!/bin/bash
# Inicia o servidor de atualização em background
# Chamado automaticamente pelo botão Atualizar nos relatórios

LOGFILE="$HOME/relatorio-benefits/server.log"
PIDFILE="$HOME/relatorio-benefits/server.pid"

# Verifica se já está rodando
if [ -f "$PIDFILE" ]; then
    PID=$(cat "$PIDFILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "Servidor já rodando (PID $PID)"
        exit 0
    fi
fi

# Inicia em background
cd "$HOME/relatorio-benefits"
nohup python3 -W ignore server.py > "$LOGFILE" 2>&1 &
echo $! > "$PIDFILE"
echo "Servidor iniciado (PID $!)"
