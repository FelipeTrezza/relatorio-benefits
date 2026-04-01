#!/usr/bin/env python3
"""
Relatório Antecipações PIX — Atualização Diária
================================================
Uso:  python3 atualizar_pix.py

2 queries paralelas por mês (atual + anterior):
  - antecip: vidas, qtd, TPV, vidas novas — filtrado cashin_destiny='PIX'
  - abertura: abertura de contas por setor

Link: https://felipetrezza.github.io/relatorio-benefits/pix.html
"""

import json, time, sys, warnings, subprocess, re
from datetime import datetime, date, timedelta
from pathlib import Path

warnings.filterwarnings("ignore")

DATABRICKS_HOST    = "https://picpay-principal.cloud.databricks.com"
DATABRICKS_PROFILE = "picpay"
WAREHOUSE_ID       = "6077a99f149e0d70"
SCRIPT_DIR         = Path(__file__).parent
TEMPLATE_PATH      = SCRIPT_DIR / "pix_template.html"
OUTPUT_PATH        = SCRIPT_DIR / "pix.html"
GITHUB_PAGES_URL   = "https://felipetrezza.github.io/relatorio-benefits/pix.html"

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

def sql_antecip(cur_mes, prev_mes, cur_ini, prev_ini, cur_day):
    """Query PIX com flags MTD embutidos — datas passadas como literais."""
    return f"""
WITH pri_antecipacao AS (
  SELECT consumer_id, min(created_at) AS dt_pri
  FROM benefits.anticipation_request
  WHERE request_status = 'FINISH'
  GROUP BY 1
),
fim AS (
  SELECT
    {SETOR_CASE} AS setor,
    ar.consumer_id,
    ar.request_value,
    CASE WHEN pa.consumer_id IS NULL THEN 0 ELSE 1 END AS fl_pri,
    CASE WHEN date_format(ar.created_at,'yyyy-MM') = '{cur_mes}'
              AND day(ar.created_at) <= {cur_day} THEN 1 ELSE 0 END AS fl_cur,
    CASE WHEN date_format(ar.created_at,'yyyy-MM') = '{prev_mes}'
              AND day(ar.created_at) <= {cur_day} THEN 1 ELSE 0 END AS fl_prv
  FROM benefits.anticipation_request ar
  LEFT JOIN benefits.companies e ON ar.company_id = e.company_id
  LEFT JOIN benefits.accounts  f ON e.account_id  = f.account_id
  LEFT JOIN pri_antecipacao pa
         ON ar.consumer_id = pa.consumer_id AND ar.created_at = pa.dt_pri
  WHERE ar.request_status = 'FINISH'
    AND ar.cashin_destiny = 'PIX'
    AND ar.created_at >= '{prev_ini}'
)
SELECT
  setor,
  count(distinct CASE WHEN fl_cur=1 THEN consumer_id END)                     AS vidas_cur,
  count(CASE WHEN fl_cur=1 THEN 1 END)                                        AS antecip_cur,
  round(sum(CASE WHEN fl_cur=1 THEN request_value ELSE 0 END), 2)             AS tpv_cur,
  count(distinct CASE WHEN fl_cur=1 AND fl_pri=1 THEN consumer_id END)        AS novas_cur,
  count(distinct CASE WHEN fl_prv=1 THEN consumer_id END)                     AS vidas_prv,
  count(CASE WHEN fl_prv=1 THEN 1 END)                                        AS antecip_prv,
  round(sum(CASE WHEN fl_prv=1 THEN request_value ELSE 0 END), 2)             AS tpv_prv,
  count(distinct CASE WHEN fl_prv=1 AND fl_pri=1 THEN consumer_id END)        AS novas_prv
FROM fim
GROUP BY 1
ORDER BY 1
"""

def sql_abertura(cur_mes, prev_mes, cur_ini, prev_ini, cur_day):
    """Abertura de contas por setor — mês atual e anterior."""
    return f"""
WITH base AS (
  SELECT sc.collaborator_document, sc.company_id
  FROM benefits.sec_collaborators sc
  INNER JOIN consumers.sec_consumers cc ON cc.cpf = sc.collaborator_document
  INNER JOIN consumers.consumers c      ON c.consumer_id = cc.consumer_id
  WHERE c.sent_bacen_at >= '{prev_ini}'
    AND c.sent_bacen_at IS NOT NULL
  QUALIFY row_number() OVER (PARTITION BY sc.collaborator_document ORDER BY sc.collaborator_id DESC) = 1
),
flagged AS (
  SELECT
    {SETOR_CASE} AS setor,
    CASE WHEN date_format(c.sent_bacen_at,'yyyy-MM') = '{cur_mes}'
              AND day(c.sent_bacen_at) <= {cur_day} THEN 1 ELSE 0 END AS fl_cur,
    CASE WHEN date_format(c.sent_bacen_at,'yyyy-MM') = '{prev_mes}'
              AND day(c.sent_bacen_at) <= {cur_day} THEN 1 ELSE 0 END AS fl_prv
  FROM base b
  INNER JOIN consumers.sec_consumers cc ON cc.cpf = b.collaborator_document
  INNER JOIN consumers.consumers c      ON c.consumer_id = cc.consumer_id
  LEFT JOIN  benefits.companies e       ON b.company_id = e.company_id
  LEFT JOIN  benefits.accounts  f       ON e.account_id = f.account_id
)
SELECT
  setor,
  sum(fl_cur) AS abertura_cur,
  sum(fl_prv) AS abertura_prv
FROM flagged
GROUP BY 1
ORDER BY 1
"""

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
def build_data(raw, cur_ini, prev_ini):
    ac = {r['setor']: r for r in raw.get('antecip',  [])}
    bc = {r['setor']: r for r in raw.get('abertura', [])}

    setores = {}
    for sk in ['public', 'private', 'grupo', 'INSS']:
        a = ac.get(sk, {}); b = bc.get(sk, {})
        qc   = int(a.get('antecip_cur',   0) or 0)
        qp   = int(a.get('antecip_prv',   0) or 0)
        ab_c = int(b.get('abertura_cur',  0) or 0)
        ab_p = int(b.get('abertura_prv',  0) or 0)
        if qc == 0 and qp == 0 and ab_c == 0 and ab_p == 0:
            continue
        setores[sk] = {
            'vidas':            int(a.get('vidas_cur',   0) or 0),
            'vidas_prv':        int(a.get('vidas_prv',   0) or 0),
            'antecipacoes':     qc,
            'antecip_prv':      qp,
            'tpv':              float(a.get('tpv_cur',   0) or 0),
            'tpv_prv':          float(a.get('tpv_prv',   0) or 0),
            'abertura':         ab_c,
            'abertura_prv':     ab_p,
            'vidas_novas':      int(a.get('novas_cur',   0) or 0),
            'vidas_novas_prv':  int(a.get('novas_prv',   0) or 0),
        }

    return {
        'cur_label':  label_mes(cur_ini),
        'prev_label': label_mes(prev_ini),
        'setores':    setores,
    }

# ── html ──────────────────────────────────────────────────────────────────────
def generate_html(data):
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    new_data = f"const DATA = {json.dumps(data, ensure_ascii=False, indent=2)};"
    html = re.sub(r'const DATA = %%DADOS%%;',      new_data, template)
    html = re.sub(r'const DATA = \{[\s\S]*?\n\};', new_data, html)
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"📄 {OUTPUT_PATH.name} ({OUTPUT_PATH.stat().st_size//1024} KB)")

# ── git push ──────────────────────────────────────────────────────────────────
def git_push():
    import os
    env = {**os.environ, "PATH": "/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:" + os.environ.get("PATH","") }
    now_str = datetime.now().strftime("%d/%m/%Y %H:%M")
    g = lambda *args: subprocess.run(["git","-C",str(SCRIPT_DIR)]+list(args), capture_output=True, text=True, env=env)

    # 1. add + commit somente o HTML gerado
    g("add","pix.html")
    rc = g("commit","-m",f"chore: pix — {now_str}")
    if rc.returncode != 0:
        if "nothing to commit" in rc.stdout+rc.stderr:
            print("   (sem mudanças)"); return
        print(f"   ⚠️  commit: {rc.stderr.strip()}")

    # 2. stash outros arquivos modificados para não bloquear o rebase
    stashed = False
    st = g("stash","--include-untracked","--keep-index")
    if st.returncode == 0 and "No local changes" not in st.stdout+st.stderr:
        stashed = True

    # 3. pull --rebase para sincronizar com remoto antes do push
    rp = g("pull","--rebase","origin","main")
    if rp.returncode != 0:
        print(f"   ⚠️  pull --rebase falhou: {rp.stderr.strip()}")

    # 4. restaurar stash
    if stashed:
        g("stash","pop")

    # 5. push
    rk = g("push","origin","main")
    if rk.returncode != 0:
        print(f"   ⚠️  push: {rk.stderr.strip()}")
    else:
        print(f"✅ {GITHUB_PAGES_URL}")

# ── main ──────────────────────────────────────────────────────────────────────
def main():
    print("="*55)
    print("  Antecipações PIX — Atualização Diária")
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
    cur_mes  = cur_ini.strftime("%Y-%m")
    prev_mes = prev_ini.strftime("%Y-%m")
    cur_day  = today.day

    print(f"\n[2/4] Período: {label_mes(cur_ini)} (dia≤{cur_day}) vs {label_mes(prev_ini)}")

    stmts = {
        "antecip":  submit(w, sql_antecip( cur_mes, prev_mes, cur_ini, prev_ini, cur_day)),
        "abertura": submit(w, sql_abertura(cur_mes, prev_mes, cur_ini, prev_ini, cur_day)),
    }
    print(f"   2 queries submetidas em paralelo")

    raw = poll_all(w, stmts)

    print("\n[3/4] Transformando dados...")
    data = build_data(raw, cur_ini, prev_ini)
    for sk, d in data['setores'].items():
        print(f"   {sk}: vidas={d['vidas']:,} antecip={d['antecipacoes']:,} tpv=R${d['tpv']:,.0f} abertura={d['abertura']:,} novas={d['vidas_novas']:,}")
    generate_html(data)

    print("\n[4/4] Publicando...")
    git_push()
    print(f"\n✅ Concluído — {datetime.now().strftime('%d/%m/%Y %H:%M')}")

if __name__ == "__main__":
    main()
