#!/usr/bin/env python3
"""
Benefits Analytics — Servidor Local de Atualização
===================================================
Uso:  python3 server.py

Deixe rodando em background. O botão "Atualizar" nos relatórios
chama este servidor via fetch(), que executa os scripts e faz push.

Endpoints:
  GET  /status          → status dos scripts (último resultado)
  POST /atualizar/antecipacoes  → roda atualizar_antecipacoes.py
  POST /atualizar/pix           → roda atualizar_pix.py
  POST /atualizar/contas        → roda atualizar.py (abertura de contas)
  POST /atualizar/todos         → roda todos em sequência
"""

import subprocess, json, threading, time, os
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

PORT      = 5001
BASE_DIR  = Path(__file__).parent
CONTAS_SCRIPT = Path.home() / "relatorio-abertura-contas" / "atualizar.py"

# estado compartilhado
state = {
    "running": False,
    "last_run": None,
    "last_status": "idle",   # idle | running | success | error
    "last_output": "",
}

SCRIPTS = {
    "antecipacoes": BASE_DIR / "atualizar_antecipacoes.py",
    "pix":          BASE_DIR / "atualizar_pix.py",
    "contas":       CONTAS_SCRIPT,
    "tv":           BASE_DIR / "atualizar_antecipacoes.py",  # tv.html é gerado junto
}

def run_script(name):
    script = SCRIPTS.get(name)
    if not script or not Path(script).exists():
        return False, f"Script não encontrado: {script}"
    result = subprocess.run(
        ["python3", "-W", "ignore", str(script)],
        capture_output=True, text=True, timeout=600
    )
    output = result.stdout + result.stderr
    return result.returncode == 0, output

def run_async(names):
    state["running"] = True
    state["last_status"] = "running"
    state["last_run"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    all_output = []
    success = True
    for name in names:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Rodando {name}...")
        ok, output = run_script(name)
        all_output.append(f"=== {name} ===\n{output}")
        if not ok:
            success = False
            print(f"  ❌ {name} falhou")
        else:
            print(f"  ✅ {name} ok")
    state["last_output"]  = "\n".join(all_output)
    state["last_status"]  = "success" if success else "error"
    state["running"]      = False

class Handler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        pass  # silencia logs do servidor

    def send_json(self, code, data):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        if self.path == "/status":
            self.send_json(200, state)
        else:
            self.send_json(404, {"error": "not found"})

    def do_POST(self):
        if state["running"]:
            self.send_json(409, {"error": "Já existe uma atualização em andamento", "state": state})
            return

        if self.path == "/atualizar/antecipacoes":
            names = ["antecipacoes"]
        elif self.path == "/atualizar/pix":
            names = ["pix"]
        elif self.path == "/atualizar/contas":
            names = ["contas"]
        elif self.path == "/atualizar/todos":
            names = ["antecipacoes", "pix", "contas"]
        else:
            self.send_json(404, {"error": "endpoint inválido"})
            return

        threading.Thread(target=run_async, args=(names,), daemon=True).start()
        self.send_json(202, {
            "message": f"Iniciando atualização: {', '.join(names)}",
            "state": state
        })

if __name__ == "__main__":
    print(f"╔══════════════════════════════════════════╗")
    print(f"║  Benefits Analytics — Servidor Local     ║")
    print(f"║  http://localhost:{PORT}                    ║")
    print(f"║  Ctrl+C para encerrar                    ║")
    print(f"╚══════════════════════════════════════════╝")
    server = HTTPServer(("localhost", PORT), Handler)
    server.serve_forever()
