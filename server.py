#!/usr/bin/env python3
"""
Benefits Analytics — Servidor Local de Atualização
===================================================
Uso:  python3 server.py

Endpoints:
  GET  /status                    → status dos scripts
  POST /atualizar/antecipacoes    → roda atualizar_antecipacoes.py
  POST /atualizar/pix             → roda atualizar_pix.py
  POST /atualizar/contas          → roda atualizar.py (abertura de contas)
  POST /atualizar/todos           → roda todos em sequência
  POST /funil                     → {"activity_name": "..."} → funil mkt x antecipações
"""

import subprocess, json, threading, os, sys
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

PORT      = 5001
BASE_DIR  = Path(__file__).parent
CONTAS_SCRIPT = Path.home() / "relatorio-abertura-contas" / "atualizar.py"
FUNIL_HELPER       = BASE_DIR / "funil_query_helper.py"
TABELAS_SCRIPT     = BASE_DIR / "verificar_tabelas.py"

state = {
    "running": False,
    "last_run": None,
    "last_status": "idle",
    "last_output": "",
}

SCRIPTS = {
    "antecipacoes": BASE_DIR / "atualizar_antecipacoes.py",
    "pix":          BASE_DIR / "atualizar_pix.py",
    "contas":       CONTAS_SCRIPT,
    "tabelas":      TABELAS_SCRIPT,
    "score":        BASE_DIR / "atualizar_score.py",
}

def get_python():
    import shutil
    return shutil.which("python3") or "/usr/bin/python3"

def get_env():
    return {**os.environ, "PATH": "/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:" + os.environ.get("PATH", "")}

def run_script(name):
    script = SCRIPTS.get(name)
    if not script or not Path(script).exists():
        return False, f"Script nao encontrado: {script}"
    result = subprocess.run(
        [get_python(), "-W", "ignore", str(script)],
        capture_output=True, text=True, timeout=600, env=get_env()
    )
    return result.returncode == 0, result.stdout + result.stderr

def run_async(names):
    state["running"] = True
    state["last_status"] = "running"
    state["last_run"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    all_output = []
    ok_all = True
    try:
        for name in names:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Rodando {name}...")
            ok, out = run_script(name)
            all_output.append(f"=== {name} ===\n{out}")
            if not ok:
                ok_all = False
        state["last_output"] = "\n".join(all_output)
        state["last_status"] = "success" if ok_all else "error"
    except Exception as e:
        state["last_output"] = str(e)
        state["last_status"] = "error"
        print(f"[ERRO run_async] {e}")
    finally:
        state["running"] = False  # SEMPRE libera, mesmo se der exceção

def run_funil_query(activity, date_from=None, date_to=None, is_journey=False):
    """Chama funil_query_helper.py como subprocess com o activity/journey e período opcional."""
    if not FUNIL_HELPER.exists():
        raise Exception(f"Helper nao encontrado: {FUNIL_HELPER}")
    cmd = [get_python(), "-W", "ignore", str(FUNIL_HELPER), activity]
    if date_from: cmd.append(date_from)
    if date_to:   cmd.append(date_to)
    if is_journey: cmd.append("--journey")
    result = subprocess.run(
        cmd,
        capture_output=True, text=True, timeout=300, env=get_env()
    )
    # Procura linha JSON no stdout (ignora warnings)
    for line in result.stdout.strip().split("\n"):
        line = line.strip()
        if line.startswith("{"):
            data = json.loads(line)
            if not data.get("ok"):
                raise Exception(data.get("error", "Erro desconhecido"))
            return data
    stderr_tail = result.stderr[-500:] if result.stderr else ""
    raise Exception(f"Sem resposta valida. stderr: {stderr_tail}")


class Handler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        pass

    def _serve_html(self, candidates):
        html_path = next((p for p in candidates if Path(p).exists()), None)
        if html_path:
            content = Path(html_path).read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", len(content))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(content)
        else:
            self.send_json(404, {"error": "HTML não encontrado"})

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
            # Auto-reset se running=True por mais de 10 minutos (estado travado)
            if state["running"] and state.get("last_run"):
                try:
                    from datetime import datetime
                    t0 = datetime.strptime(state["last_run"], "%d/%m/%Y %H:%M:%S")
                    elapsed = (datetime.now() - t0).total_seconds()
                    if elapsed > 600:  # 10 min
                        state["running"] = False
                        state["last_status"] = "error"
                        state["last_output"] += "\n[auto-reset após 10min sem conclusão]"
                except Exception:
                    pass
            self.send_json(200, {"ok": True, "status": state["last_status"], "running": state["running"],
                                 "last_run": state.get("last_run"), "last_output": state.get("last_output", "")})
        elif self.path == "/log":
            self.send_json(200, {"last_run": state.get("last_run"), "last_status": state.get("last_status"),
                                 "last_output": state.get("last_output", "")})
        elif self.path in ("/antecipacoes", "/antecipacoes.html"):
            self._serve_html([BASE_DIR / "antecipacoes.html"])
        elif self.path in ("/", "/score", "/score.html", "/index.html"):
            self._serve_html([
                Path.home() / "score-antecipacoes" / "index.html",
                BASE_DIR / "antecipacoes.html",
            ])
        elif self.path in ("/pix", "/pix.html"):
            self._serve_html([BASE_DIR / "pix.html"])
        else:
            self.send_json(404, {"error": "not found"})

    def do_POST(self):
        # Reset de emergência — desbloqueia estado running travado
        if self.path == "/reset":
            state["running"] = False
            state["last_status"] = "idle"
            self.send_json(200, {"ok": True, "message": "estado resetado"})
            return

        # Endpoint funil é sempre síncrono (não bloqueia state)
        if self.path == "/funil":
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)
            try:
                payload = json.loads(body)
                # Aceita activity_name (string) ou activities (lista)
                journey_val = payload.get("journey", "").strip()
                raw = payload.get("activities") or payload.get("activity_name") or payload.get("activity")
                if journey_val:
                    # Busca por journey — delega direto ao helper com --journey
                    activities = [journey_val]
                    payload["_is_journey"] = True
                elif not raw:
                    self.send_json(400, {"error": "activities ou journey obrigatorio"})
                    return
                else:
                    activities = [raw] if isinstance(raw, str) else [a.strip() for a in raw if a.strip()]
                    payload["_is_journey"] = False
                if not activities:
                    self.send_json(400, {"error": "Nenhum activity/journey valido"})
                    return
            except Exception:
                self.send_json(400, {"error": "JSON invalido"})
                return

            date_from = payload.get("date_from", "").strip() or None
            date_to   = payload.get("date_to",   "").strip() or None

            if len(activities) == 1:
                # consulta simples
                act = activities[0]
                period_str = f"{date_from or 'auto'} → {date_to or 'auto'}"
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Funil: {act[:60]}... ({period_str})")
                try:
                    result = run_funil_query(act, date_from, date_to, is_journey=payload.get("_is_journey", False))
                    result["multi"] = False
                    print(f"  -> {len(result.get('rows', []))} linhas | {result.get('date_from')} → {result.get('date_to')}")
                    self.send_json(200, result)
                except Exception as e:
                    print(f"  -> ERRO: {e}")
                    self.send_json(500, {"error": str(e)})
            else:
                # consulta múltipla — paralela
                from concurrent.futures import ThreadPoolExecutor, as_completed
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Funil multi: {len(activities)} activities...")
                results = {}
                errors  = {}
                date_from = payload.get("date_from", "").strip() or None
                date_to   = payload.get("date_to",   "").strip() or None
                with ThreadPoolExecutor(max_workers=min(len(activities), 4)) as ex:
                    future_map = {ex.submit(run_funil_query, a, date_from, date_to, payload.get("_is_journey", False)): a for a in activities}
                    for future in as_completed(future_map):
                        act = future_map[future]
                        try:
                            results[act] = future.result()
                            print(f"  -> OK: {act[:50]} ({len(results[act].get('rows',[]))} linhas)")
                        except Exception as e:
                            errors[act] = str(e)
                            print(f"  -> ERRO: {act[:50]}: {e}")
                self.send_json(200, {"multi": True, "activities": activities, "results": results, "errors": errors})
            return

        if state["running"]:
            self.send_json(409, {"error": "Atualizacao em andamento", "state": state})
            return

        if self.path == "/atualizar/antecipacoes":
            names = ["antecipacoes"]
        elif self.path == "/atualizar/pix":
            names = ["pix"]
        elif self.path == "/atualizar/contas":
            names = ["contas"]
        elif self.path == "/atualizar/todos":
            names = ["antecipacoes", "pix", "contas", "tabelas"]
        elif self.path == "/atualizar/tabelas":
            names = ["tabelas"]
        elif self.path == "/atualizar/score":
            names = ["score"]
        else:
            self.send_json(404, {"error": "endpoint invalido"})
            return

        threading.Thread(target=run_async, args=(names,), daemon=True).start()
        self.send_json(202, {"message": f"Iniciando: {', '.join(names)}", "state": state})


if __name__ == "__main__":
    HTTPServer.allow_reuse_address = True
    server = HTTPServer(("localhost", PORT), Handler)
    print("╔════════════════════════════════════════════════════╗")
    print(f"║  Benefits Analytics — Servidor Local               ║")
    print(f"║                                                    ║")
    print(f"║  👉 http://localhost:{PORT}/score                    ║")
    print(f"║                                                    ║")
    print(f"║  Deixe esta janela aberta. Ctrl+C para encerrar.  ║")
    print("╚════════════════════════════════════════════════════╝")
    server.serve_forever()
