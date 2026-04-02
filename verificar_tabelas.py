#!/usr/bin/env python3
"""
Verificador de Atualização de Tabelas — Benefits Analytics
============================================================
Consulta a data mais recente de atualização de cada tabela monitorada
e gera um relatório HTML gerencial.

Uso:  python3 verificar_tabelas.py

Link: https://felipetrezza.github.io/relatorio-benefits/tabelas.html
"""

import json, time, sys, warnings, subprocess
from datetime import datetime, date, timedelta
from pathlib import Path

warnings.filterwarnings("ignore")

DATABRICKS_HOST    = "https://picpay-principal.cloud.databricks.com"
DATABRICKS_PROFILE = "picpay"
WAREHOUSE_ID       = "6077a99f149e0d70"
SCRIPT_DIR         = Path(__file__).parent
OUTPUT_PATH        = SCRIPT_DIR / "tabelas.html"
GITHUB_REPO_DIR    = Path.home() / "relatorio-benefits"

# Arquivo que persiste o snapshot de benefits.accounts entre execuções
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

# ── Lógica especial: benefits.accounts ────────────────────────────────────────
# Essa tabela não tem movimentação diária. O alerta só dispara quando o número
# de linhas muda (inserção → ok, remoção → erro). Caso contrário, sempre "Estável".

ACCOUNTS_KEY = "benefits.accounts"

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

def status_badge(max_date_str, error=None):
    """Retorna (label, css_class) baseado na defasagem."""
    if error:
        return "Erro", "status-error"
    if not max_date_str or max_date_str in ("None", "null", ""):
        return "Sem dados", "status-error"
    try:
        # Tenta parsear data/datetime
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(str(max_date_str)[:19], fmt)
                break
            except ValueError:
                continue
        else:
            return "Formato inválido", "status-warn"

        delta = (datetime.now() - dt).total_seconds() / 3600  # horas
        if delta <= 26:
            return "Atualizado", "status-ok"
        elif delta <= 72:
            return f"{int(delta/24)}d atrás", "status-warn"
        else:
            return f"{int(delta/24)}d atrás", "status-error"
    except Exception as e:
        return "Inválido", "status-warn"

def fmt_date(val):
    if not val or str(val) in ("None", "null", ""):
        return "—"
    try:
        dt = datetime.strptime(str(val)[:19], "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%d/%m/%Y %H:%M")
    except:
        try:
            dt = datetime.strptime(str(val)[:10], "%Y-%m-%d")
            return dt.strftime("%d/%m/%Y")
        except:
            return str(val)[:19]

def fmt_rows(val):
    try:
        return f"{int(float(str(val))):,}".replace(",", ".")
    except:
        return "—"

def generate_html(results):
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
        <tr>
          <td class="col-table">
            <span class="table-name">{display_name}</span>
            <span class="table-desc">{description}</span>
            {variation_html}
          </td>
          <td class="col-col"><code>{date_col}</code></td>
          <td class="col-date">{date_display}</td>
          <td class="col-rows">{rows_display}</td>
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
  <meta http-equiv="refresh" content="300">
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

    /* ── Summary cards ── */
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
    }}
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
    .sum-card.health .sc-val {{ color: {health_color}; font-size: 22px; }}

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
        <div class="updated-at" style="margin-top:3px">Auto-refresh a cada 5 min</div>
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
    <div class="sum-card ok">
      <div class="sc-val">{ok_count}</div>
      <div class="sc-lbl">✅ Atualizadas</div>
    </div>
    <div class="sum-card warn">
      <div class="sc-val">{warn_count}</div>
      <div class="sc-lbl">⚠️ Com atraso</div>
    </div>
    <div class="sum-card err">
      <div class="sc-val">{error_count}</div>
      <div class="sc-lbl">❌ Com problema</div>
    </div>
    <div class="sum-card" style="border-color:var(--border)">
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
    <table>
      <thead>
        <tr>
          <th>Tabela</th>
          <th>Coluna monitorada</th>
          <th>Última atualização</th>
          <th style="text-align:right">Total linhas</th>
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
    cmds = [
        ["git", "-C", str(GITHUB_REPO_DIR), "add", "tabelas.html"],
        ["git", "-C", str(GITHUB_REPO_DIR), "commit", "-m",
         f"chore: atualiza monitor de tabelas {datetime.now().strftime('%d/%m/%Y %H:%M')}"],
        ["git", "-C", str(GITHUB_REPO_DIR), "push"],
    ]
    for cmd in cmds:
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0 and "nothing to commit" not in r.stdout + r.stderr:
            print(f"   ⚠️  git: {r.stderr.strip()[:100]}")

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*60}")
    print(f"  Monitor de Tabelas — {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"{'='*60}")

    print("\n[1/4] Conectando ao Databricks...")
    w = get_w()
    print("   ✅ Conexão OK")

    print(f"\n[2/4] Submetendo {len(TABELAS)} queries em paralelo...")
    stmts = {}
    for (display_name, full_table, date_col, _) in TABELAS:
        sql = build_query(full_table, date_col)
        stmts[display_name] = submit(w, sql)
        print(f"   → {display_name}")

    print("\n[3/4] Aguardando resultados...")
    results = poll_all(w, stmts)

    print("\n[4/4] Gerando HTML e publicando...")
    html = generate_html(results)
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"   ✅ Salvo em {OUTPUT_PATH}")

    git_push()
    print(f"\n✅ Concluído — https://felipetrezza.github.io/relatorio-benefits/tabelas.html")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
