#!/usr/bin/env python3
"""
Verificador de Atualização de Tabelas — Benefits Analytics
============================================================
Consulta a data mais recente de atualização de cada tabela monitorada
e gera um relatório HTML gerencial.

Uso:  python3 verificar_tabelas.py

Pré-requisitos:
  pip install databricks-sdk

Autenticação (escolha uma das opções):
  Opção 1 — Profile local (recomendado):
    databricks auth login --host https://picpay-principal.cloud.databricks.com --profile picpay
    (o script usa DATABRICKS_PROFILE abaixo automaticamente)

  Opção 2 — Variáveis de ambiente:
    export DATABRICKS_HOST=https://picpay-principal.cloud.databricks.com
    export DATABRICKS_TOKEN=<seu_token>
    export DATABRICKS_WAREHOUSE_ID=<seu_warehouse_id>   # opcional, sobrescreve o padrão

  Opção 3 — Default credentials (se já autenticado via CLI sem profile):
    Basta rodar sem configuração adicional.

Saída:
  tabelas.html  →  gerado na mesma pasta do script
  (se o diretório for um repo git com acesso de push, publica automaticamente)
"""

import json, os, time, warnings, subprocess
from datetime import datetime, date
from pathlib import Path

warnings.filterwarnings("ignore")

# ── Configuração ───────────────────────────────────────────────────────────────
# Pode ser sobrescrito pela variável de ambiente DATABRICKS_HOST
DATABRICKS_HOST    = os.environ.get("DATABRICKS_HOST",
                                    "https://picpay-principal.cloud.databricks.com")

# Profile do Databricks CLI (usado se DATABRICKS_TOKEN não estiver no ambiente)
DATABRICKS_PROFILE = os.environ.get("DATABRICKS_PROFILE", "picpay")

# Warehouse padrão PicPay — pode ser sobrescrito por DATABRICKS_WAREHOUSE_ID
WAREHOUSE_ID       = os.environ.get("DATABRICKS_WAREHOUSE_ID", "6077a99f149e0d70")

SCRIPT_DIR         = Path(__file__).parent
OUTPUT_PATH        = SCRIPT_DIR / "tabelas.html"

# Snapshot de benefits.accounts — salvo junto ao script
ACCOUNTS_SNAPSHOT  = SCRIPT_DIR / "accounts_snapshot.json"

# ── Tabelas monitoradas ────────────────────────────────────────────────────────
# Formato: (nome_exibição, schema.tabela, coluna_de_data, descrição)
TABELAS = [
    (
        "benefits.collaborators",
        "benefits.collaborators",
        "updated_at",
        "Base de colaboradores elegíveis ao benefício"
    ),
    (
        "benefits.companies",
        "benefits.companies",
        "updated_at",
        "Cadastro de empresas conveniadas"
    ),
    (
        "benefits.accounts",
        "benefits.accounts",
        "updated_at",
        "Contas de empresas no Benefits"
    ),
    (
        "consumers.consumers",
        "consumers.consumers",
        "sent_bacen_at",
        "Base de consumidores PicPay"
    ),
    (
        "benefits.anticipation_request",
        "benefits.anticipation_request",
        "created_at",
        "Solicitações de antecipação de salário"
    ),
    (
        "benefits.sec_wage_advance_collaborator_margins",
        "benefits.sec_wage_advance_collaborator_margins",
        "updated_at",
        "Margens de antecipação por colaborador"
    ),
    (
        "consumers.daily_consumers_labels_and_metrics",
        "consumers.daily_consumers_labels_and_metrics",
        "metric_date",
        "Métricas diárias de consumidores (MAU, labels)"
    ),
    (
        "marketing.consumers_campaigns_communications",
        "marketing.consumers_campaigns_communications",
        "sent_at",
        "Comunicações de campanhas de marketing"
    ),
    (
        "benefits.all_movements",
        "benefits.all_movements",
        "created_at",
        "Movimentações consolidadas Benefits"
    ),
    (
        "picpay.all_transactions",
        "picpay.picpay.all_transactions",
        "created_at",
        "Transações consolidadas PicPay"
    ),
    (
        "benefits.benefits_purchases",
        "benefits.benefits_purchases",
        "created_at",
        "Compras realizadas via Benefits"
    ),
]

# ── Databricks helpers ─────────────────────────────────────────────────────────

def get_w():
    from databricks.sdk import WorkspaceClient
    # Se DATABRICKS_TOKEN estiver no ambiente, usa direto (sem precisar de profile)
    if os.environ.get("DATABRICKS_TOKEN"):
        return WorkspaceClient(host=DATABRICKS_HOST)
    # Caso contrário, usa o profile configurado via CLI
    return WorkspaceClient(host=DATABRICKS_HOST, profile=DATABRICKS_PROFILE)

def submit(w, sql):
    r = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=sql, wait_timeout="0s")
    return r.statement_id

def poll_all(w, stmts, timeout=300):
    from databricks.sdk.service.sql import StatementState
    results = {}
    pending = dict(stmts)
    elapsed = 0
    while pending and elapsed < timeout:
        time.sleep(8)
        elapsed += 8
        done = []
        for key, sid in pending.items():
            s = w.statement_execution.get_statement(sid)
            st = s.status.state
            if st == StatementState.SUCCEEDED:
                cols = [c.name for c in s.manifest.schema.columns]
                rows = [dict(zip(cols, r)) for r in (s.result.data_array or [])]
                results[key] = rows[0] if rows else {}
                print(f"   ✅ {key}")
                done.append(key)
            elif st in (StatementState.FAILED, StatementState.CANCELED):
                err = s.status.error.message if s.status.error else "erro desconhecido"
                results[key] = {"error": err}
                print(f"   ❌ {key}: {err[:80]}")
                done.append(key)
        for k in done:
            del pending[k]
        if pending:
            print(f"   [{elapsed}s] aguardando {len(pending)} queries...", end="\r")
    # timeout restantes
    for k in list(pending.keys()):
        results[k] = {"error": "timeout"}
    return results

# ── Queries ────────────────────────────────────────────────────────────────────

def build_query(full_table, date_col):
    """Monta query para pegar a data mais recente + contagem total."""
    return f"""
SELECT
  max({date_col})      AS max_date,
  count(*)             AS total_rows
FROM {full_table}
"""

def build_historico_query(full_table, date_expr, extra_filter=""):
    """Monta query de contagem por dia nos últimos 7 dias."""
    return f"""
SELECT
  {date_expr} AS dia,
  count(*)    AS n
FROM {full_table}
WHERE {date_expr} >= current_date() - 7
  AND {date_expr} < current_date()
  {extra_filter}
GROUP BY 1
ORDER BY 1
"""

def fetch_historico(w, dias=7):
    """
    Submete queries de histórico diário para todas as tabelas configuradas.
    Retorna dict: key → lista de {'dia': 'YYYY-MM-DD', 'n': int} ou {'error': msg}
    Queries com timeout individual não bloqueiam as demais.
    """
    from databricks.sdk.service.sql import StatementState
    from datetime import timedelta

    # Dias esperados: D-7 até D-1 (hoje ainda pode estar incompleto)
    today = date.today()
    dias_esperados = [(today - timedelta(days=i)).isoformat() for i in range(dias, 0, -1)]

    stmts = {}
    for key, (full_table, date_expr) in HISTORICO_TABELAS.items():
        extra = "AND is_current = true" if key == "picpay.all_transactions" else ""
        sql = build_historico_query(full_table, date_expr, extra)
        try:
            sid = submit(w, sql)
            stmts[key] = sid
        except Exception as e:
            stmts[key] = None
            print(f"   ⚠️  histórico {key}: erro ao submeter — {e}")

    # Poll com timeout individual por tabela
    timeout_default = 90
    resultados = {}
    pending = {k: v for k, v in stmts.items() if v}
    elapsed = 0
    timeouts = {}  # key → tempo máximo em segundos

    while pending and elapsed < 240:
        time.sleep(10)
        elapsed += 10
        done = []
        for key, sid in pending.items():
            limite = HISTORICO_TIMEOUT.get(key, timeout_default)
            s = w.statement_execution.get_statement(sid)
            st = s.status.state
            if st == StatementState.SUCCEEDED:
                cols = [c.name for c in s.manifest.schema.columns]
                rows = [{"dia": r[0], "n": int(r[1])} for r in (s.result.data_array or [])]
                # Preenche dias faltantes com n=0 (gap real)
                por_dia = {r["dia"]: r["n"] for r in rows}
                resultados[key] = [
                    {"dia": d, "n": por_dia.get(d, 0), "gap": d not in por_dia}
                    for d in dias_esperados
                ]
                print(f"   ✅ histórico {key}")
                done.append(key)
            elif st.name in ("FAILED", "CANCELED"):
                err = s.status.error.message[:80] if s.status.error else "erro"
                resultados[key] = {"error": err}
                print(f"   ❌ histórico {key}: {err}")
                done.append(key)
            elif elapsed >= limite:
                # Timeout individual — cancela a query
                try:
                    w.statement_execution.cancel_execution(sid)
                except Exception:
                    pass
                resultados[key] = {"error": f"timeout ({limite}s)"}
                print(f"   ⏱️  histórico {key}: timeout após {limite}s")
                done.append(key)
        for k in done:
            del pending[k]
        if pending:
            print(f"   [{elapsed}s] histórico aguardando: {len(pending)}...", end="\r")

    # Qualquer pendente restante
    for key in list(pending.keys()):
        resultados[key] = {"error": "timeout global"}

    # Tabelas sem histórico (accounts)
    for (display_name, _, _, _) in TABELAS:
        if display_name not in resultados and display_name not in HISTORICO_TABELAS:
            resultados[display_name] = None  # sem histórico

    return resultados, dias_esperados

# ── Lógica especial: benefits.accounts ────────────────────────────────────────
# Essa tabela não tem movimentação diária. O alerta só dispara quando o número
# de linhas muda (inserção → ok, remoção → erro). Caso contrário, sempre "Estável".

ACCOUNTS_KEY = "benefits.accounts"

# Tabelas com histórico diário + coluna de data para agrupar
# (nome_key, full_table, date_col)
# Tabelas sem histórico (accounts) ficam de fora
HISTORICO_TABELAS = {
    "benefits.collaborators":                        ("benefits.collaborators",                        "date(updated_at)"),
    "benefits.companies":                            ("benefits.companies",                            "date(updated_at)"),
    "consumers.consumers":                           ("consumers.consumers",                           "date(sent_bacen_at)"),
    "benefits.anticipation_request":                 ("benefits.anticipation_request",                 "date(created_at)"),
    "benefits.sec_wage_advance_collaborator_margins":("benefits.sec_wage_advance_collaborator_margins","date(updated_at)"),
    "consumers.daily_consumers_labels_and_metrics":  ("consumers.daily_consumers_labels_and_metrics",  "metric_date"),
    "marketing.consumers_campaigns_communications":  ("marketing.consumers_campaigns_communications",  "date(sent_at)"),
    "benefits.all_movements":                        ("benefits.all_movements",                        "date(created_at)"),
    "picpay.all_transactions":                       ("picpay.picpay.all_transactions",                "date(created_at)"),
    "benefits.benefits_purchases":                   ("benefits.benefits_purchases",                   "date(created_at)"),
}

# Timeout individual por tabela (segundos) — tabelas pesadas têm mais tempo
HISTORICO_TIMEOUT = {
    "benefits.collaborators":                         120,
    "benefits.sec_wage_advance_collaborator_margins": 120,
    "consumers.consumers":                            120,
    "picpay.all_transactions":                        180,
    "consumers.daily_consumers_labels_and_metrics":   120,
    "marketing.consumers_campaigns_communications":   120,
}

def load_accounts_snapshot():
    """Lê o snapshot salvo da última execução. Retorna dict com 'count'."""
    if ACCOUNTS_SNAPSHOT.exists():
        try:
            return json.loads(ACCOUNTS_SNAPSHOT.read_text())
        except Exception:
            pass
    return None

def save_accounts_snapshot(count: int):
    """Persiste o count atual para comparação amanhã."""
    ACCOUNTS_SNAPSHOT.write_text(json.dumps({
        "count": count,
        "checked_at": date.today().isoformat()
    }))

def accounts_status(current_count: int):
    """
    Compara current_count com o snapshot salvo.
    Retorna (label, css_class, detail_msg).
    """
    snap = load_accounts_snapshot()

    if snap is None:
        # Primeira execução — apenas salva, não alarma
        save_accounts_snapshot(current_count)
        return "Estável", "status-ok", f"{current_count} empresas · primeira leitura registrada"

    prev_count = int(snap.get("count", current_count))
    diff = current_count - prev_count

    # Salva snapshot atualizado independentemente do resultado
    save_accounts_snapshot(current_count)

    if diff == 0:
        return "Estável", "status-ok", f"{current_count} empresas · sem alterações"
    elif diff > 0:
        plural = "linha adicionada" if diff == 1 else "linhas adicionadas"
        return f"+{diff} {plural}", "status-ok", f"{current_count} empresas (era {prev_count})"
    else:
        plural = "linha removida" if abs(diff) == 1 else "linhas removidas"
        return f"{abs(diff)} {plural} ⚠️", "status-error", f"{current_count} empresas (era {prev_count})"

# ── HTML ───────────────────────────────────────────────────────────────────────

def parse_dt(val):
    """Converte qualquer formato de data/datetime retornado pelo Databricks em datetime."""
    if not val or str(val) in ("None", "null", ""):
        return None
    s = str(val).strip()
    # Remove sufixo Z ou timezone (+00:00) e milissegundos antes de parsear
    s = s.rstrip("Z").split("+")[0].split(".")[0]  # ex: "2026-04-02T07:51:19"
    s = s.replace("T", " ")                         # ex: "2026-04-02 07:51:19"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None

def status_badge(max_date_str, error=None):
    """Retorna (label, css_class) baseado na defasagem."""
    if error:
        return "Erro", "status-error"
    dt = parse_dt(max_date_str)
    if dt is None:
        return "Sem dados", "status-error"
    delta = (datetime.now() - dt).total_seconds() / 3600  # horas
    if delta <= 26:
        return "Atualizado", "status-ok"
    elif delta <= 72:
        return f"{int(delta/24)}d atrás", "status-warn"
    else:
        return f"{int(delta/24)}d atrás", "status-error"

def fmt_date(val):
    dt = parse_dt(val)
    if dt is None:
        return "—"
    # Exibe hora só se não for meia-noite exata (datas sem hora)
    if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
        return dt.strftime("%d/%m/%Y")
    return dt.strftime("%d/%m/%Y %H:%M")

def fmt_rows(val):
    try:
        return f"{int(float(str(val))):,}".replace(",", ".")
    except:
        return "—"

def render_sparkbar(hist_data, dias_esperados):
    """Gera o HTML do mini gráfico de barras para uma tabela."""
    if hist_data is None:
        return '<span class="spark-na">—</span>'
    if isinstance(hist_data, dict) and "error" in hist_data:
        return f'<span class="spark-na" title="{hist_data["error"]}">indisponível</span>'

    valores = [d["n"] for d in hist_data]
    max_val = max(valores) if any(v > 0 for v in valores) else 1
    today = date.today().isoformat()
    ontem = (date.today() - __import__("datetime").timedelta(days=1)).isoformat()

    bars = ""
    tem_gap = False
    for d in hist_data:
        dia_str = d["dia"]
        n = d["n"]
        is_gap = d.get("gap", False) and n == 0
        is_today = dia_str == today

        if is_gap:
            tem_gap = True

        altura = max(3, round((n / max_val) * 26)) if n > 0 else 3
        css_bar = "spark-bar gap" if is_gap else ("spark-bar today" if is_today else "spark-bar")

        # Formata label do dia ex: "02/04"
        try:
            dt = __import__("datetime").date.fromisoformat(dia_str)
            label_dia = dt.strftime("%d/%m")
        except Exception:
            label_dia = dia_str[-5:]

        n_fmt = f"{n:,}".replace(",", ".")
        tip = f"{label_dia}: {'faltando dados' if is_gap else n_fmt}"

        bars += f'<div class="spark-day" data-tip="{tip}"><div class="{css_bar}" style="height:{altura}px"></div></div>'

    return f'<div class="sparkbar" title="{"⚠️ dia(s) sem dados" if tem_gap else "últimos 7 dias"}">{bars}</div>'


def generate_html(results, historico, dias_esperados):
    now_str = datetime.now().strftime("%d/%m/%Y às %H:%M")
    today = date.today()

    rows_html = ""
    ok_count = warn_count = error_count = 0

    for (display_name, full_table, date_col, description) in TABELAS:
        key = display_name
        r = results.get(key, {})
        error = r.get("error")
        max_date = r.get("max_date")
        total_rows = r.get("total_rows")

        # ── Lógica especial para benefits.accounts ──
        if key == ACCOUNTS_KEY and not error:
            current_count = int(float(str(total_rows))) if total_rows else 0
            label, css, detail_msg = accounts_status(current_count)

            # "última atualização" mostra a data real da tabela
            date_display = fmt_date(max_date) if max_date else "—"
            rows_display = f'<span title="{detail_msg}">{fmt_rows(total_rows)}</span>'

            # Exibe detalhe de variação abaixo do nome
            variation_html = f'<span class="table-desc accounts-detail">{detail_msg}</span>'

        else:
            label, css = status_badge(max_date, error)
            date_display = fmt_date(max_date) if not error else f'<span class="err-msg">{error[:60]}</span>'
            rows_display = "—" if error else fmt_rows(total_rows)
            variation_html = ""

        if css == "status-ok":
            ok_count += 1
        elif css == "status-warn":
            warn_count += 1
        else:
            error_count += 1

        rows_html += f"""
        <tr data-status="{css}">
          <td class="col-table">
            <span class="table-name">{display_name}</span>
            <span class="table-desc">{description}</span>
            {variation_html}
          </td>
          <td class="col-col"><code>{date_col}</code></td>
          <td class="col-date">{date_display}</td>
          <td class="col-rows">{rows_display}</td>
          <td class="col-spark">{render_sparkbar(historico.get(display_name), dias_esperados)}</td>
          <td class="col-status"><span class="badge {css}">{label}</span></td>
        </tr>"""

    total = len(TABELAS)
    pct_ok = round(ok_count / total * 100)

    health_color = "#21a366" if error_count == 0 else ("#f0883e" if warn_count > 0 and error_count <= 2 else "#f85149")
    health_label = "Saudável" if error_count == 0 else ("Atenção" if error_count <= 2 else "Crítico")

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Monitor de Tabelas — Benefits Analytics</title>
  <style>
    :root {{
      --bg: #0d1117;
      --surface: #161b22;
      --surf2: #1c2330;
      --border: #30363d;
      --green: #21a366;
      --greenl: #2dc974;
      --blue: #58a6ff;
      --yellow: #e3b341;
      --orange: #f0883e;
      --red: #f85149;
      --muted: #8b949e;
      --text: #e6edf3;
      --text2: #c9d1d9;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--text);
      min-height: 100vh;
      padding: 0 0 60px;
    }}

    /* ── Header ── */
    .header {{
      background: var(--surface);
      border-bottom: 1px solid var(--border);
      padding: 20px 32px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      position: sticky;
      top: 0;
      z-index: 100;
    }}
    .header-left {{ display: flex; align-items: center; gap: 14px; }}
    .header-icon {{ font-size: 28px; }}
    .header-title {{ font-size: 18px; font-weight: 700; color: var(--text); }}
    .header-sub {{ font-size: 12px; color: var(--muted); margin-top: 2px; }}
    .header-right {{ text-align: right; display: flex; align-items: center; gap: 16px; }}
    .updated-at {{ font-size: 12px; color: var(--muted); text-align: right; }}
    .updated-at strong {{ color: var(--text2); }}

    /* ── Botão Atualizar ── */
    #btn-atualizar {{
      display: inline-flex;
      align-items: center;
      gap: 7px;
      padding: 8px 18px;
      background: var(--green);
      color: #fff;
      border: none;
      border-radius: 8px;
      font-size: 13px;
      font-weight: 600;
      cursor: pointer;
      transition: background .2s, transform .1s, opacity .2s;
      white-space: nowrap;
    }}
    #btn-atualizar:hover:not(:disabled) {{ background: var(--greenl); transform: translateY(-1px); }}
    #btn-atualizar:active {{ transform: translateY(0); }}
    #btn-atualizar:disabled {{ opacity: .55; cursor: not-allowed; }}
    #btn-atualizar svg {{ flex-shrink: 0; }}
    #btn-atualizar.loading svg {{ animation: spin 1s linear infinite; }}
    @keyframes spin {{ to {{ transform: rotate(360deg); }} }}

    /* ── Toast ── */
    #toast {{
      position: fixed;
      bottom: 28px;
      right: 28px;
      padding: 12px 20px;
      border-radius: 10px;
      font-size: 13px;
      font-weight: 500;
      color: #fff;
      opacity: 0;
      pointer-events: none;
      transition: opacity .3s;
      z-index: 9999;
      max-width: 360px;
    }}
    #toast.show {{ opacity: 1; }}
    #toast.toast-ok   {{ background: rgba(33,163,102,.9);  border: 1px solid rgba(33,163,102,.6); }}
    #toast.toast-warn {{ background: rgba(227,179,65,.9);  border: 1px solid rgba(227,179,65,.6); color: #0d1117; }}
    #toast.toast-err  {{ background: rgba(248,81,73,.88);  border: 1px solid rgba(248,81,73,.6); }}

    /* ── Summary cards — filtro clicável ── */
    .summary {{
      display: flex;
      gap: 16px;
      padding: 24px 32px 0;
      flex-wrap: wrap;
    }}
    .sum-card {{
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 16px 24px;
      min-width: 160px;
      flex: 1;
      cursor: pointer;
      user-select: none;
      transition: border-color .2s, transform .15s, box-shadow .2s;
      position: relative;
    }}
    .sum-card:not(.health):hover {{
      transform: translateY(-2px);
      box-shadow: 0 4px 16px rgba(0,0,0,.4);
    }}
    .sum-card.active-filter {{
      box-shadow: 0 0 0 2px currentColor;
    }}
    .sum-card.ok.active-filter   {{ box-shadow: 0 0 0 2px var(--green); border-color: var(--green); }}
    .sum-card.warn.active-filter {{ box-shadow: 0 0 0 2px var(--yellow); border-color: var(--yellow); }}
    .sum-card.err.active-filter  {{ box-shadow: 0 0 0 2px var(--red); border-color: var(--red); }}
    .sum-card .sc-val {{
      font-size: 32px;
      font-weight: 800;
      line-height: 1;
    }}
    .sum-card .sc-lbl {{
      font-size: 12px;
      color: var(--muted);
      margin-top: 6px;
      text-transform: uppercase;
      letter-spacing: .6px;
    }}
    .sum-card.ok   .sc-val {{ color: var(--green); }}
    .sum-card.warn .sc-val {{ color: var(--yellow); }}
    .sum-card.err  .sc-val {{ color: var(--red); }}
    .sum-card.health {{ cursor: default; }}
    .sum-card.health .sc-val {{ color: {health_color}; font-size: 22px; }}
    /* hint de filtro */
    .sum-card:not(.health)::after {{
      content: "clique para filtrar";
      position: absolute;
      bottom: 8px;
      right: 12px;
      font-size: 9px;
      color: var(--muted);
      opacity: 0;
      transition: opacity .2s;
      letter-spacing: .4px;
      text-transform: uppercase;
    }}
    .sum-card:not(.health):hover::after {{ opacity: 1; }}
    /* linha oculta pelo filtro */
    tbody tr.hidden-row {{ display: none; }}
    /* banner de filtro ativo */
    #filter-banner {{
      display: none;
      margin: 16px 32px 0;
      padding: 10px 16px;
      background: var(--surf2);
      border: 1px solid var(--border);
      border-radius: 8px;
      font-size: 12px;
      color: var(--text2);
      align-items: center;
      gap: 10px;
    }}
    #filter-banner.visible {{ display: flex; }}
    #filter-banner button {{
      margin-left: auto;
      background: none;
      border: 1px solid var(--border);
      color: var(--muted);
      border-radius: 5px;
      padding: 3px 10px;
      font-size: 11px;
      cursor: pointer;
    }}
    #filter-banner button:hover {{ color: var(--text); border-color: var(--text2); }}

    /* ── Health bar ── */
    .health-bar-wrap {{
      padding: 20px 32px 0;
    }}
    .health-bar-bg {{
      background: var(--surf2);
      border-radius: 6px;
      height: 8px;
      overflow: hidden;
    }}
    .health-bar-fill {{
      height: 100%;
      border-radius: 6px;
      background: {health_color};
      width: {pct_ok}%;
      transition: width .5s ease;
    }}
    .health-label {{
      font-size: 11px;
      color: var(--muted);
      margin-top: 6px;
      text-align: right;
    }}

    /* ── Table ── */
    .table-wrap {{
      padding: 24px 32px 0;
      overflow-x: auto;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13.5px;
    }}
    thead tr {{
      background: var(--surf2);
    }}
    thead th {{
      padding: 10px 14px;
      text-align: left;
      font-size: 11px;
      font-weight: 600;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: .6px;
      border-bottom: 1px solid var(--border);
    }}
    tbody tr {{
      border-bottom: 1px solid var(--border);
      transition: background .15s;
    }}
    tbody tr:hover {{
      background: var(--surface);
    }}
    td {{
      padding: 12px 14px;
      vertical-align: middle;
    }}
    .col-table {{ min-width: 260px; }}
    .col-col   {{ min-width: 160px; }}
    .col-date  {{ min-width: 150px; white-space: nowrap; }}
    .col-rows  {{ min-width: 110px; text-align: right; color: var(--muted); font-variant-numeric: tabular-nums; }}
    .col-status {{ min-width: 110px; text-align: center; }}

    .table-name {{
      display: block;
      font-weight: 600;
      color: var(--blue);
      font-family: "SFMono-Regular", Consolas, monospace;
      font-size: 13px;
    }}
    .table-desc {{
      display: block;
      font-size: 11.5px;
      color: var(--muted);
      margin-top: 2px;
    }}
    .accounts-detail {{
      color: var(--text2);
      font-style: italic;
      margin-top: 3px;
    }}

    /* ── Sparkbar histórico ── */
    .sparkbar {{
      display: flex;
      align-items: flex-end;
      gap: 3px;
      height: 28px;
    }}
    .spark-day {{
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 2px;
      flex: 1;
      position: relative;
    }}
    .spark-bar {{
      width: 100%;
      min-height: 3px;
      border-radius: 2px 2px 0 0;
      background: var(--green);
      opacity: .75;
      transition: opacity .15s;
    }}
    .spark-bar.gap {{
      background: var(--red);
      opacity: .9;
      animation: blink-bar 1.5s ease-in-out infinite;
    }}
    @keyframes blink-bar {{ 0%,100%{{ opacity:.9; }} 50%{{ opacity:.4; }} }}
    .spark-bar.today {{
      background: var(--blue);
      opacity: .6;
    }}
    .spark-day:hover .spark-bar {{ opacity: 1; }}
    /* tooltip */
    .spark-day::after {{
      content: attr(data-tip);
      position: absolute;
      bottom: calc(100% + 4px);
      left: 50%;
      transform: translateX(-50%);
      background: #1c2330;
      border: 1px solid var(--border);
      color: var(--text);
      font-size: 10px;
      white-space: nowrap;
      padding: 3px 7px;
      border-radius: 5px;
      pointer-events: none;
      opacity: 0;
      z-index: 50;
      transition: opacity .15s;
    }}
    .spark-day:hover::after {{ opacity: 1; }}
    .spark-na {{
      font-size: 10px;
      color: var(--muted);
      white-space: nowrap;
    }}
    .col-spark {{ min-width: 120px; }}
    code {{
      font-family: "SFMono-Regular", Consolas, monospace;
      font-size: 12px;
      color: var(--orange);
      background: rgba(240,136,62,.1);
      padding: 2px 6px;
      border-radius: 4px;
    }}

    /* ── Badges ── */
    .badge {{
      display: inline-block;
      padding: 3px 10px;
      border-radius: 20px;
      font-size: 11.5px;
      font-weight: 700;
      letter-spacing: .4px;
    }}
    .status-ok    {{ background: rgba(33,163,102,.18); color: #2dc974; border: 1px solid rgba(33,163,102,.4); }}
    .status-warn  {{ background: rgba(227,179,65,.15);  color: #e3b341; border: 1px solid rgba(227,179,65,.4); }}
    .status-error {{ background: rgba(248,81,73,.15);   color: #f85149; border: 1px solid rgba(248,81,73,.4); }}
    .err-msg {{ color: var(--red); font-size: 11.5px; }}

    /* ── Footer ── */
    .footer {{
      padding: 28px 32px 0;
      font-size: 11px;
      color: var(--muted);
      display: flex;
      align-items: center;
      gap: 8px;
    }}
    .dot {{ width: 6px; height: 6px; border-radius: 50%; background: var(--green); display: inline-block; animation: pulse 2s infinite; }}
    @keyframes pulse {{ 0%,100%{{ opacity:1; }} 50%{{ opacity:.3; }} }}
  </style>
</head>
<body>

  <div class="header">
    <div class="header-left">
      <span class="header-icon">🗄️</span>
      <div>
        <div class="header-title">Monitor de Tabelas — Benefits Analytics</div>
        <div class="header-sub">Acompanhamento diário de atualização das fontes de dados</div>
      </div>
    </div>
    <div class="header-right">
      <div>
        <div class="updated-at">Gerado em <strong>{now_str}</strong></div>

      </div>
      <button id="btn-atualizar" onclick="atualizarTabelas()">
        <svg width="14" height="14" viewBox="0 0 16 16" fill="currentColor">
          <path d="M8 3a5 5 0 1 0 4.546 2.914.5.5 0 0 1 .908-.417A6 6 0 1 1 8 2v1z"/>
          <path d="M8 4.466V.534a.25.25 0 0 1 .41-.192l2.36 1.966c.12.1.12.284 0 .384L8.41 4.658A.25.25 0 0 1 8 4.466z"/>
        </svg>
        Atualizar
      </button>
    </div>
  </div>

  <div class="summary">
    <div class="sum-card health">
      <div class="sc-val">{health_label}</div>
      <div class="sc-lbl">Status geral</div>
    </div>
    <div class="sum-card ok" onclick="filtrar('status-ok', this)" title="Filtrar tabelas atualizadas">
      <div class="sc-val">{ok_count}</div>
      <div class="sc-lbl">✅ Atualizadas</div>
    </div>
    <div class="sum-card warn" onclick="filtrar('status-warn', this)" title="Filtrar tabelas com atraso">
      <div class="sc-val">{warn_count}</div>
      <div class="sc-lbl">⚠️ Com atraso</div>
    </div>
    <div class="sum-card err" onclick="filtrar('status-error', this)" title="Filtrar tabelas com problema">
      <div class="sc-val">{error_count}</div>
      <div class="sc-lbl">❌ Com problema</div>
    </div>
    <div class="sum-card" style="border-color:var(--border); cursor:default">
      <div class="sc-val" style="color:var(--text2)">{total}</div>
      <div class="sc-lbl">Total monitoradas</div>
    </div>
  </div>

  <div class="health-bar-wrap">
    <div class="health-bar-bg">
      <div class="health-bar-fill"></div>
    </div>
    <div class="health-label">{pct_ok}% das tabelas atualizadas</div>
  </div>

  <div class="table-wrap">
    <div id="filter-banner">
      <span id="filter-label"></span>
      <button onclick="limparFiltro()">✕ Limpar filtro</button>
    </div>
    <table>
      <thead>
        <tr>
          <th>Tabela</th>
          <th>Coluna monitorada</th>
          <th>Última atualização</th>
          <th style="text-align:right">Total linhas</th>
          <th>Últimos 7 dias</th>
          <th style="text-align:center">Status</th>
        </tr>
      </thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>
  </div>

  <div class="footer">
    <span class="dot"></span>
    Critérios: <strong style="color:var(--green)">Atualizado</strong> = ≤26h &nbsp;|&nbsp;
    <strong style="color:var(--yellow)">Atraso</strong> = 26h–72h &nbsp;|&nbsp;
    <strong style="color:var(--red)">Problema</strong> = &gt;72h ou erro
  </div>

  <!-- Toast -->
  <div id="toast"></div>

  <script>
    var filtroAtivo = null;

    var LABELS = {{
      'status-ok':    '✅ Exibindo apenas: Atualizadas',
      'status-warn':  '⚠️ Exibindo apenas: Com atraso',
      'status-error': '❌ Exibindo apenas: Com problema'
    }};

    function filtrar(status, card) {{
      // Toggle: clicou no mesmo filtro ativo → limpa
      if (filtroAtivo === status) {{
        limparFiltro();
        return;
      }}
      filtroAtivo = status;

      // Atualiza destaque nos cards
      document.querySelectorAll('.sum-card').forEach(function(c) {{
        c.classList.remove('active-filter');
      }});
      card.classList.add('active-filter');

      // Mostra/oculta linhas
      document.querySelectorAll('tbody tr').forEach(function(tr) {{
        tr.classList.toggle('hidden-row', tr.dataset.status !== status);
      }});

      // Banner
      var banner = document.getElementById('filter-banner');
      document.getElementById('filter-label').textContent = LABELS[status] || status;
      banner.classList.add('visible');
    }}

    function limparFiltro() {{
      filtroAtivo = null;
      document.querySelectorAll('.sum-card').forEach(function(c) {{
        c.classList.remove('active-filter');
      }});
      document.querySelectorAll('tbody tr').forEach(function(tr) {{
        tr.classList.remove('hidden-row');
      }});
      document.getElementById('filter-banner').classList.remove('visible');
    }}

    function toast(msg, tipo) {{
      var t = document.getElementById('toast');
      t.textContent = msg;
      t.className = 'show toast-' + tipo;
      clearTimeout(t._timer);
      t._timer = setTimeout(function() {{ t.className = ''; }}, 4500);
    }}

    function atualizarTabelas() {{
      var btn = document.getElementById('btn-atualizar');
      btn.disabled = true;
      btn.classList.add('loading');
      btn.innerHTML = '<svg width="14" height="14" viewBox="0 0 16 16" fill="currentColor"><path d="M8 3a5 5 0 1 0 4.546 2.914.5.5 0 0 1 .908-.417A6 6 0 1 1 8 2v1z"/><path d="M8 4.466V.534a.25.25 0 0 1 .41-.192l2.36 1.966c.12.1.12.284 0 .384L8.41 4.658A.25.25 0 0 1 8 4.466z"/></svg> Atualizando…';

      // Testa se o servidor local está rodando
      fetch('http://localhost:5001/status', {{ signal: AbortSignal.timeout(3000) }})
        .then(function(r) {{ return r.json(); }})
        .then(function() {{
          // Servidor OK — dispara a atualização
          return fetch('http://localhost:5001/atualizar/tabelas', {{ method: 'POST' }});
        }})
        .then(function(r) {{ return r.json(); }})
        .then(function() {{
          toast('⏳ Atualização iniciada! Aguarde ~1 min e a página recarregará.', 'ok');
          // Polling: verifica a cada 8s se o script terminou (max 3 min)
          var tentativas = 0;
          var poll = setInterval(function() {{
            tentativas++;
            fetch('http://localhost:5001/status')
              .then(function(r) {{ return r.json(); }})
              .then(function(s) {{
                if (!s.running) {{
                  clearInterval(poll);
                  toast('✅ Tabelas atualizadas! Recarregando…', 'ok');
                  setTimeout(function() {{ location.reload(); }}, 1500);
                }} else if (tentativas >= 22) {{
                  clearInterval(poll);
                  toast('⚠️ Demorou mais que o esperado. Recarregue a página em breve.', 'warn');
                  resetBtn();
                }}
              }})
              .catch(function() {{ clearInterval(poll); resetBtn(); }});
          }}, 8000);
        }})
        .catch(function(err) {{
          // Servidor não está rodando
          var instrucao = err && err.name === 'AbortError'
            ? 'Servidor local não encontrado.'
            : 'Servidor local não encontrado.';
          toast(
            '🔌 ' + instrucao + ' Para usar o botão, rode no terminal:\\n' +
            'cd ~/relatorio-benefits && python3 server.py',
            'err'
          );
          resetBtn();
        }});
    }}

    function resetBtn() {{
      var btn = document.getElementById('btn-atualizar');
      btn.disabled = false;
      btn.classList.remove('loading');
      btn.innerHTML = '<svg width="14" height="14" viewBox="0 0 16 16" fill="currentColor"><path d="M8 3a5 5 0 1 0 4.546 2.914.5.5 0 0 1 .908-.417A6 6 0 1 1 8 2v1z"/><path d="M8 4.466V.534a.25.25 0 0 1 .41-.192l2.36 1.966c.12.1.12.284 0 .384L8.41 4.658A.25.25 0 0 1 8 4.466z"/></svg> Atualizar';
    }}
  </script>

</body>
</html>"""
    return html

# ── Git push ───────────────────────────────────────────────────────────────────

def git_push():
    """Tenta publicar o HTML via git. Silencioso se não for um repo ou sem acesso de push."""
    repo_dir = SCRIPT_DIR

    # Verifica se é um repo git
    check = subprocess.run(
        ["git", "-C", str(repo_dir), "rev-parse", "--is-inside-work-tree"],
        capture_output=True, text=True
    )
    if check.returncode != 0:
        print("   ℹ️  Não é um repositório git — HTML salvo localmente apenas.")
        return

    cmds = [
        ["git", "-C", str(repo_dir), "add", "tabelas.html", "accounts_snapshot.json"],
        ["git", "-C", str(repo_dir), "commit", "-m",
         f"chore: atualiza monitor de tabelas {datetime.now().strftime('%d/%m/%Y %H:%M')}"],
        ["git", "-C", str(repo_dir), "push"],
    ]
    for cmd in cmds:
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            msg = r.stdout + r.stderr
            if "nothing to commit" in msg:
                pass  # normal, sem mudanças
            elif "push" in cmd:
                print("   ℹ️  Push não realizado (sem acesso ou remote não configurado).")
                print(f"      HTML disponível em: {OUTPUT_PATH}")
            else:
                print(f"   ⚠️  git: {msg.strip()[:120]}")

# ── Main ───────────────────────────────────────────────────────────────────────

def check_deps():
    """Verifica dependências e orienta instalação se faltar algo."""
    try:
        import databricks.sdk
    except ImportError:
        print("\n❌ Dependência faltando: databricks-sdk")
        print("   Instale com:  pip install databricks-sdk")
        print("   ou:           pip3 install databricks-sdk\n")
        raise SystemExit(1)

def main():
    print(f"\n{'='*60}")
    print(f"  Monitor de Tabelas — {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"{'='*60}")

    check_deps()

    print("\n[1/4] Conectando ao Databricks...")
    try:
        w = get_w()
        # Teste rápido de conectividade
        w.current_user.me()
        print("   ✅ Conexão OK")
    except Exception as e:
        msg = str(e)
        print(f"\n❌ Falha na autenticação: {msg[:200]}")
        print("\n   Para configurar o acesso, escolha uma opção:")
        print("   1) Profile local (recomendado):")
        print("      databricks auth login --host https://picpay-principal.cloud.databricks.com --profile picpay")
        print("   2) Variáveis de ambiente:")
        print("      export DATABRICKS_HOST=https://picpay-principal.cloud.databricks.com")
        print("      export DATABRICKS_TOKEN=<seu_token>")
        raise SystemExit(1)

    print(f"\n[2/5] Submetendo {len(TABELAS)} queries de status em paralelo...")
    stmts = {}
    for (display_name, full_table, date_col, _) in TABELAS:
        sql = build_query(full_table, date_col)
        stmts[display_name] = submit(w, sql)
        print(f"   → {display_name}")

    print(f"\n[3/5] Aguardando resultados de status...")
    results = poll_all(w, stmts)

    print(f"\n[4/5] Buscando histórico diário (últimos 7 dias)...")
    historico, dias_esperados = fetch_historico(w)

    print("\n[5/5] Gerando HTML e publicando...")
    html = generate_html(results, historico, dias_esperados)
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"   ✅ Salvo em {OUTPUT_PATH}")

    git_push()
    print(f"\n✅ Concluído — https://felipetrezza.github.io/relatorio-benefits/tabelas.html")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
