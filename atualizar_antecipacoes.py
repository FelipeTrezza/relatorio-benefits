#!/usr/bin/env python3
"""
Relatório Antecipações de Salário — Atualização Diária
=======================================================
Uso:  python3 atualizar_antecipacoes.py

Queries (6 em paralelo):
  - antecipações: vidas, qtd, TPV por setor × (mês atual | mês anterior)
  - vidas novas:  1ª antecipação ever, dentro do mês × setor
  - abertura de contas: via sec_collaborators × setor

Link: https://felipetrezza.github.io/relatorio-benefits/antecipacoes.html
"""

import json, time, sys, warnings, subprocess, re
from datetime import datetime, date, timedelta
from pathlib import Path

warnings.filterwarnings("ignore")

DATABRICKS_HOST    = "https://picpay-principal.cloud.databricks.com"
DATABRICKS_PROFILE = "picpay"
WAREHOUSE_ID       = "6077a99f149e0d70"
SCRIPT_DIR         = Path(__file__).parent
TEMPLATE_PATH      = SCRIPT_DIR / "antecipacoes_template.html"
OUTPUT_PATH        = SCRIPT_DIR / "antecipacoes.html"
GITHUB_PAGES_URL   = "https://felipetrezza.github.io/relatorio-benefits/antecipacoes.html"

MESES_PT = {'Jan':'Jan','Feb':'Fev','Mar':'Mar','Apr':'Abr','May':'Mai',
            'Jun':'Jun','Jul':'Jul','Aug':'Ago','Sep':'Set','Oct':'Out','Nov':'Nov','Dec':'Dez'}
def label_mes(d):
    s = d.strftime("%b/%y").capitalize()
    for en, pt in MESES_PT.items(): s = s.replace(en, pt)
    return s

SETOR_CASE = """
  CASE
    WHEN f.account_id = 1420 THEN 'INSS'
    WHEN f.account_id IN (6,52,57,58,60,62,65,73,100,3170,3675) THEN 'grupo'
    WHEN f.account_id IS NULL THEN 'INSS'
    ELSE coalesce(e.flag_company_sector, 'private')
  END
"""

SQL_ANTECIP = """
SELECT
  {setor} AS setor,
  count(distinct ar.consumer_id)                      AS vidas,
  count(*)                                            AS antecipacoes,
  round(sum(cast(ar.request_value AS double)), 2)     AS tpv
FROM benefits.anticipation_request ar
LEFT JOIN benefits.companies e ON ar.company_id = e.company_id
LEFT JOIN benefits.accounts  f ON e.account_id  = f.account_id
WHERE ar.request_status = 'FINISH'
  AND ar.created_at >= '{{ini}}'
  AND ar.created_at <  '{{fim}}'
  AND day(ar.created_at) <= {{d}}
GROUP BY 1
""".format(setor=SETOR_CASE)

SQL_NOVAS = """
SELECT
  {setor} AS setor,
  count(distinct ar.consumer_id) AS vidas_novas
FROM benefits.anticipation_request ar
LEFT JOIN benefits.companies e ON ar.company_id = e.company_id
LEFT JOIN benefits.accounts  f ON e.account_id  = f.account_id
WHERE ar.request_status = 'FINISH'
  AND ar.created_at >= '{{ini}}'
  AND ar.created_at <  '{{fim}}'
  AND day(ar.created_at) <= {{d}}
  AND ar.consumer_id NOT IN (
    SELECT DISTINCT consumer_id
    FROM benefits.anticipation_request
    WHERE request_status = 'FINISH'
      AND created_at < '{{ini}}'
  )
GROUP BY 1
""".format(setor=SETOR_CASE)

SQL_ABERTURA = """
WITH base AS (
  SELECT
    sc.collaborator_document,
    sc.company_id
  FROM benefits.sec_collaborators sc
  INNER JOIN consumers.sec_consumers cc ON cc.cpf = sc.collaborator_document
  INNER JOIN consumers.consumers c      ON c.consumer_id = cc.consumer_id
  WHERE c.sent_bacen_at >= '{{ini}}'
    AND c.sent_bacen_at <  '{{fim}}'
    AND day(c.sent_bacen_at) <= {{d}}
    AND c.sent_bacen_at IS NOT NULL
  QUALIFY row_number() OVER (PARTITION BY sc.collaborator_document ORDER BY sc.collaborator_id DESC) = 1
)
SELECT
  {setor} AS setor,
  count(*) AS abertura_contas
FROM base b
LEFT JOIN benefits.companies e ON b.company_id = e.company_id
LEFT JOIN benefits.accounts  f ON e.account_id = f.account_id
GROUP BY 1
""".format(setor=SETOR_CASE)

# SQLs com filtro GDF/RJ (account_id NOT IN 892, 1817)
SQL_ANTECIP2 = """
SELECT
  {setor} AS setor,
  count(distinct ar.consumer_id)                      AS vidas,
  count(*)                                            AS antecipacoes,
  round(sum(cast(ar.request_value AS double)), 2)     AS tpv
FROM benefits.anticipation_request ar
LEFT JOIN benefits.companies e ON ar.company_id = e.company_id
LEFT JOIN benefits.accounts  f ON e.account_id  = f.account_id
WHERE ar.request_status = 'FINISH'
  AND ar.created_at >= '{{ini}}'
  AND ar.created_at <  '{{fim}}'
  AND day(ar.created_at) <= {{d}}
  AND f.account_id NOT IN (892, 1817)
GROUP BY 1
""".format(setor=SETOR_CASE)

SQL_NOVAS2 = """
SELECT
  {setor} AS setor,
  count(distinct ar.consumer_id) AS vidas_novas
FROM benefits.anticipation_request ar
LEFT JOIN benefits.companies e ON ar.company_id = e.company_id
LEFT JOIN benefits.accounts  f ON e.account_id  = f.account_id
WHERE ar.request_status = 'FINISH'
  AND ar.created_at >= '{{ini}}'
  AND ar.created_at <  '{{fim}}'
  AND day(ar.created_at) <= {{d}}
  AND f.account_id NOT IN (892, 1817)
  AND ar.consumer_id NOT IN (
    SELECT DISTINCT consumer_id
    FROM benefits.anticipation_request
    WHERE request_status = 'FINISH'
      AND created_at < '{{ini}}'
  )
GROUP BY 1
""".format(setor=SETOR_CASE)

SQL_ABERTURA2 = """
WITH base AS (
  SELECT sc.collaborator_document, sc.company_id
  FROM benefits.sec_collaborators sc
  INNER JOIN consumers.sec_consumers cc ON cc.cpf = sc.collaborator_document
  INNER JOIN consumers.consumers c      ON c.consumer_id = cc.consumer_id
  WHERE c.sent_bacen_at >= '{{ini}}'
    AND c.sent_bacen_at <  '{{fim}}'
    AND day(c.sent_bacen_at) <= {{d}}
    AND c.sent_bacen_at IS NOT NULL
  QUALIFY row_number() OVER (PARTITION BY sc.collaborator_document ORDER BY sc.collaborator_id DESC) = 1
)
SELECT
  {setor} AS setor,
  count(*) AS abertura_contas
FROM base b
LEFT JOIN benefits.companies e ON b.company_id = e.company_id
LEFT JOIN benefits.accounts  f ON e.account_id = f.account_id
WHERE f.account_id NOT IN (892, 1817)
GROUP BY 1
""".format(setor=SETOR_CASE)

def fmt(sql, ini, fim, d):
    return sql.replace("{ini}", str(ini)).replace("{fim}", str(fim)).replace("{d}", str(d))

# ── auth ──────────────────────────────────────────────────────────────────────
def check_auth():
    r = subprocess.run(
        ["databricks","auth","token","--host",DATABRICKS_HOST,"--profile",DATABRICKS_PROFILE],
        capture_output=True, text=True)
    try: return r.returncode == 0 and "access_token" in json.loads(r.stdout)
    except: return False

def do_login():
    print("🔐 Browser OAuth — clique em Allow e volte aqui...")
    subprocess.run(["databricks","auth","login","--host",DATABRICKS_HOST,
                    "--profile",DATABRICKS_PROFILE], check=True)

# ── execute ───────────────────────────────────────────────────────────────────
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
        time.sleep(10); elapsed += 10
        done = []
        for key, sid in pending.items():
            s = w.statement_execution.get_statement(sid)
            st = s.status.state
            if st == StatementState.SUCCEEDED:
                cols = [c.name for c in s.manifest.schema.columns]
                results[key] = [dict(zip(cols, r)) for r in (s.result.data_array or [])]
                print(f"   ✅ {key}: {len(results[key])} linhas")
                done.append(key)
            elif st in (StatementState.FAILED, StatementState.CANCELED):
                results[key] = []
                print(f"   ❌ {key}: {s.status.error.message if s.status.error else '?'}")
                done.append(key)
        for k in done: del pending[k]
        if pending:
            print(f"   [{elapsed}s] aguardando: {list(pending.keys())}", end="\r")
    return results

# ── transform ─────────────────────────────────────────────────────────────────
def build_data(raw, cur_ini, prev_ini, cur_day):
    def idx(rows): return {r['setor']: r for r in rows}

    ac  = idx(raw.get('antecip_cur',  []))
    ap  = idx(raw.get('antecip_prev', []))
    nc  = idx(raw.get('novas_cur',    []))
    np_ = idx(raw.get('novas_prev',   []))
    bc  = idx(raw.get('abertura_cur', []))
    bp  = idx(raw.get('abertura_prev',[]))

    setores = {}
    for sk in ['public', 'private', 'grupo', 'INSS']:
        q_c = int((ac.get(sk) or {}).get('antecipacoes', 0) or 0)
        q_p = int((ap.get(sk) or {}).get('antecipacoes', 0) or 0)
        ab_c = int((bc.get(sk) or {}).get('abertura_contas', 0) or 0)
        ab_p = int((bp.get(sk) or {}).get('abertura_contas', 0) or 0)
        # inclui setor se tiver antecipações OU abertura de conta
        if q_c == 0 and q_p == 0 and ab_c == 0 and ab_p == 0:
            continue
        setores[sk] = {
            'vidas':            int((ac.get(sk) or {}).get('vidas', 0) or 0),
            'vidas_prv':        int((ap.get(sk) or {}).get('vidas', 0) or 0),
            'antecipacoes':     q_c,
            'antecip_prv':      q_p,
            'tpv':              float((ac.get(sk) or {}).get('tpv', 0) or 0),
            'tpv_prv':          float((ap.get(sk) or {}).get('tpv', 0) or 0),
            'abertura':         ab_c,
            'abertura_prv':     ab_p,
            'vidas_novas':      int((nc.get(sk) or {}).get('vidas_novas', 0) or 0),
            'vidas_novas_prv':  int((np_.get(sk) or {}).get('vidas_novas', 0) or 0),
        }

    return {
        'cur_label':  label_mes(cur_ini),
        'prev_label': label_mes(prev_ini),
        'setores':    setores,
    }

# ── html ──────────────────────────────────────────────────────────────────────
def generate_html(data, data2):
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    new_data  = f"const DATA  = {json.dumps(data,  ensure_ascii=False, indent=2)};"
    new_data2 = f"const DATA2 = {json.dumps(data2, ensure_ascii=False, indent=2)};"
    html = re.sub(r'const DATA = %%DADOS%%;',   new_data,  template)
    html = re.sub(r'const DATA2 = %%DADOS2%%;', new_data2, html)
    html = re.sub(r'const DATA = \{[\s\S]*?\n\};',  new_data,  html)
    html = re.sub(r'const DATA2 = \{[\s\S]*?\n\};', new_data2, html)
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"📄 {OUTPUT_PATH.name} ({OUTPUT_PATH.stat().st_size//1024} KB)")

# ── git push ──────────────────────────────────────────────────────────────────
def git_push():
    import os
    env = {**os.environ, "PATH": "/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:" + os.environ.get("PATH","") }
    now_str = datetime.now().strftime("%d/%m/%Y %H:%M")
    for cmd in [
        ["git","-C",str(SCRIPT_DIR),"add","antecipacoes.html","tv.html"],
        ["git","-C",str(SCRIPT_DIR),"commit","-m",f"chore: antecipacoes — {now_str}"],
        ["git","-C",str(SCRIPT_DIR),"push","origin","main"],
    ]:
        r = subprocess.run(cmd, capture_output=True, text=True, env=env)
        if r.returncode != 0:
            if "nothing to commit" in r.stdout+r.stderr: print("   (sem mudanças)"); break
            print(f"   ⚠️  {r.stderr.strip()}")
        elif "push" in cmd:
            print(f"✅ {GITHUB_PAGES_URL}")

# ── main ──────────────────────────────────────────────────────────────────────
def main():
    print("="*55)
    print("  Antecipações — Atualização Diária")
    print(f"  {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("="*55)

    print("\n[1/4] Auth Databricks...")
    if not check_auth():
        do_login()
        if not check_auth(): sys.exit("❌ Auth falhou")
    print("   ✅ ok")

    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(host=DATABRICKS_HOST, profile=DATABRICKS_PROFILE)

    today    = date.today()
    cur_ini  = today.replace(day=1)
    prev_ini = (cur_ini - timedelta(days=1)).replace(day=1)
    cur_fim  = cur_ini.replace(month=cur_ini.month % 12 + 1) if cur_ini.month < 12 \
               else cur_ini.replace(year=cur_ini.year+1, month=1)
    prev_fim = cur_ini
    cur_day  = today.day

    print(f"\n[2/4] Período: {label_mes(cur_ini)} (dia≤{cur_day}) vs {label_mes(prev_ini)}")

    stmts = {}
    # 6 queries visão principal
    for periodo, ini, fim in [("cur", cur_ini, cur_fim), ("prev", prev_ini, prev_fim)]:
        for nome, sql in [("antecip",SQL_ANTECIP),("novas",SQL_NOVAS),("abertura",SQL_ABERTURA)]:
            stmts[f"{nome}_{periodo}"] = submit(w, fmt(sql, ini, fim, cur_day))
    # 6 queries réplica sem GDF/RJ
    for periodo, ini, fim in [("cur", cur_ini, cur_fim), ("prev", prev_ini, prev_fim)]:
        for nome, sql in [("antecip2",SQL_ANTECIP2),("novas2",SQL_NOVAS2),("abertura2",SQL_ABERTURA2)]:
            stmts[f"{nome}_{periodo}"] = submit(w, fmt(sql, ini, fim, cur_day))
    print(f"   {len(stmts)} queries submetidas em paralelo (6 + 6)")

    raw = poll_all(w, stmts)

    print("\n[3/4] Transformando dados...")
    # Visão principal
    data = build_data(raw, cur_ini, prev_ini, cur_day)
    for sk, d in data['setores'].items():
        print(f"   {sk}: vidas={d['vidas']:,} antecip={d['antecipacoes']:,} tpv=R${d['tpv']:,.0f} abertura={d['abertura']:,} novas={d['vidas_novas']:,}")
    # Réplica sem GDF/RJ — remap keys: antecip2_* → antecip_* etc
    raw2 = {k.replace('antecip2_','antecip_').replace('novas2_','novas_').replace('abertura2_','abertura_'): v
            for k, v in raw.items() if '2_' in k}
    data2 = build_data(raw2, cur_ini, prev_ini, cur_day)
    print(f"   [sem GDF/RJ] público: {data2['setores'].get('public',{}).get('antecipacoes',0):,} antecip")
    generate_html(data, data2)
    generate_tv(data, data2)

    print("\n[4/4] Publicando...")
    git_push()
    print(f"\n✅ Concluído — {datetime.now().strftime('%d/%m/%Y %H:%M')}")

if __name__ == "__main__":
    main()

# ── gerar tv.html (chamado após generate_html) ────────────────────────────────
def generate_tv(data, data2):
    """Gera tv.html com DATA + DATA2 + DATA_PIX do pix.html atual."""
    import re as _re
    TV_TEMPLATE = SCRIPT_DIR / "tv_template.html"
    TV_OUTPUT   = SCRIPT_DIR / "tv.html"
    PIX_HTML    = SCRIPT_DIR / "pix.html"

    if not TV_TEMPLATE.exists():
        print("   ⚠️  tv_template.html não encontrado, pulando tv.html")
        return

    def extract(html, varname):
        m = _re.search(rf'const {varname}\s*=\s*(\{{[\s\S]*?\n\}});', html)
        return m.group(1) if m else 'null'

    # DATA_PIX vem do pix.html já gerado
    pix_html = PIX_HTML.read_text(encoding="utf-8") if PIX_HTML.exists() else ""
    data_pix = extract(pix_html, 'DATA') if pix_html else 'null'

    now_str  = datetime.now().strftime('%d/%m/%Y %H:%M')
    template = TV_TEMPLATE.read_text(encoding="utf-8")

    data_block  = json.dumps(data,  ensure_ascii=False, indent=2)
    data2_block = json.dumps(data2, ensure_ascii=False, indent=2)

    tv_html = (template
        .replace('%%DATA%%',     data_block)
        .replace('%%DATA2%%',    data2_block)
        .replace('%%DATA_PIX%%', data_pix)
        .replace('%%UPDATED%%',  now_str))

    TV_OUTPUT.write_text(tv_html, encoding="utf-8")
    print(f"📺 tv.html gerado ({TV_OUTPUT.stat().st_size//1024} KB)")
