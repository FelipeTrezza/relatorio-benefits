#!/usr/bin/env python3
"""
Relatório Antecipações de Salário — Atualização Diária
=======================================================
Uso:  python3 atualizar_antecipacoes.py

1. Conecta Databricks via OAuth
2. Roda 3 queries em paralelo (range A: ago-nov/25 | range B: dez/25+ | primeiras)
3. Combina, transforma → objeto DATA
4. Injeta no antecipacoes_template.html → antecipacoes.html
5. git push → GitHub Pages

Link: https://felipetrezza.github.io/relatorio-benefits/antecipacoes.html
"""

import json, time, sys, warnings, subprocess, re
from datetime import datetime, date, timedelta
from collections import defaultdict
from pathlib import Path

warnings.filterwarnings("ignore")

DATABRICKS_HOST    = "https://picpay-principal.cloud.databricks.com"
DATABRICKS_PROFILE = "picpay"
WAREHOUSE_ID       = "6077a99f149e0d70"
SCRIPT_DIR         = Path(__file__).parent
TEMPLATE_PATH      = SCRIPT_DIR / "antecipacoes_template.html"
OUTPUT_PATH        = SCRIPT_DIR / "antecipacoes.html"
GITHUB_PAGES_URL   = "https://felipetrezza.github.io/relatorio-benefits/antecipacoes.html"

# ── Queries ───────────────────────────────────────────────────────────────────
# Nota: tabela particionada de forma que queries com range > ~4 meses + joins
# sofrem dynamic partition pruning. Solução: dividir em 2 ranges de ~4 meses.

SQL_RANGE = """
SELECT
  date_format(ar.created_at, 'yyyy-MM')     AS mes,
  date_format(ar.created_at, 'yyyy-MM-dd')  AS dia,
  CASE
    WHEN f.account_id = 1420 THEN 'INSS'
    WHEN f.account_id IN (6,52,57,58,60,62,65,73,100,3170,3675) THEN 'grupo'
    WHEN f.account_id IS NULL THEN 'INSS'
    ELSE coalesce(e.flag_company_sector, 'private')
  END                                        AS setor,
  coalesce(upper(f.account_name), 'N/I')    AS secretaria,
  coalesce(upper(e.company_name), 'N/I')    AS empresa,
  count(*)                                  AS qtd,
  round(sum(cast(ar.request_value AS double)), 2)     AS valor_total,
  round(avg(cast(ar.anticipation_fee AS double)), 2)  AS fee_medio,
  count(distinct ar.consumer_id)            AS usuarios_unicos
FROM benefits.anticipation_request ar
LEFT JOIN benefits.companies e ON ar.company_id = e.company_id
LEFT JOIN benefits.accounts  f ON e.account_id  = f.account_id
WHERE ar.request_status = 'FINISH'
  AND ar.created_at >= '{inicio}'
  AND ar.created_at <  '{fim}'
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3
"""

SQL_PRIM = """
SELECT mes, count(*) AS primeiras
FROM (
  SELECT date_format(min(created_at),'yyyy-MM') AS mes
  FROM benefits.anticipation_request
  WHERE request_status = 'FINISH'
  GROUP BY consumer_id
  HAVING min(created_at) >= '{inicio}'
) sub
GROUP BY 1 ORDER BY 1
"""

# ── Auth ──────────────────────────────────────────────────────────────────────
def check_auth():
    r = subprocess.run(["databricks","auth","token","--host",DATABRICKS_HOST,"--profile",DATABRICKS_PROFILE],
                       capture_output=True, text=True)
    if r.returncode == 0:
        try: return "access_token" in json.loads(r.stdout)
        except: return False
    return False

def do_login():
    print("🔐 Browser OAuth — clique em Allow e volte aqui...")
    subprocess.run(["databricks","auth","login","--host",DATABRICKS_HOST,"--profile",DATABRICKS_PROFILE], check=True)

# ── Execute SQL ───────────────────────────────────────────────────────────────
def submit(w, sql):
    r = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=sql, wait_timeout="0s")
    return r.statement_id

def poll(w, stmt_id, label="", timeout=600):
    from databricks.sdk.service.sql import StatementState
    elapsed = 0
    while elapsed < timeout:
        time.sleep(15); elapsed += 15
        s = w.statement_execution.get_statement(stmt_id)
        st = s.status.state
        print(f"   [{elapsed:>3}s] {label} {st}     ", end="\r", flush=True)
        if st == StatementState.SUCCEEDED:
            print(f"\n   ✅ {label} ok ({elapsed}s)")
            cols = [c.name for c in s.manifest.schema.columns]
            return [dict(zip(cols, r)) for r in (s.result.data_array or [])]
        if st in (StatementState.FAILED, StatementState.CANCELED):
            err = s.status.error.message if s.status.error else "?"
            raise RuntimeError(f"{label} falhou: {err}")
    raise TimeoutError(f"{label} timeout {timeout}s")

# ── Transform → DATA ──────────────────────────────────────────────────────────
def transform(rows, prim_por_mes):
    today    = date.today()
    cur_mes  = today.strftime("%Y-%m")
    prev_mes = (today.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    cur_day  = today.day

    by_mes   = defaultdict(lambda: {"qtd":0,"valor":0.0})
    all_emp  = defaultdict(lambda: {"setor":"","qtd":0,"valor":0.0})
    mtd_s    = defaultdict(lambda: {"cur":{"qtd":0,"valor":0.0},"prv":{"qtd":0,"valor":0.0}})
    mtd_ec   = defaultdict(lambda: {"qtd":0,"setor":""})
    mtd_ep   = defaultdict(lambda: {"qtd":0,"setor":""})

    for r in rows:
        mes   = r["mes"]; dn = int(r["dia"].split("-")[2])
        sk    = r["setor"]; emp = r["empresa"]
        qtd   = int(r["qtd"] or 0); val = float(r["valor_total"] or 0)

        by_mes[mes]["qtd"]  += qtd; by_mes[mes]["valor"] += val
        all_emp[emp]["setor"] = sk; all_emp[emp]["qtd"] += qtd; all_emp[emp]["valor"] += val

        if mes == cur_mes and dn <= cur_day:
            mtd_s[sk]["cur"]["qtd"] += qtd; mtd_s[sk]["cur"]["valor"] += val
            mtd_ec[emp]["qtd"] += qtd; mtd_ec[emp]["setor"] = sk
        if mes == prev_mes and dn <= cur_day:
            mtd_s[sk]["prv"]["qtd"] += qtd; mtd_s[sk]["prv"]["valor"] += val
            mtd_ep[emp]["qtd"] += qtd; mtd_ep[emp]["setor"] = sk

    mtd_u_c = sum(int(r["usuarios_unicos"] or 0) for r in rows
        if r["mes"]==cur_mes  and int(r["dia"].split("-")[2])<=cur_day)
    mtd_u_p = sum(int(r["usuarios_unicos"] or 0) for r in rows
        if r["mes"]==prev_mes and int(r["dia"].split("-")[2])<=cur_day)

    all_meses     = sorted(by_mes.keys())
    mensal_labels = [m[5:]+"/"+m[2:4] for m in all_meses]
    mensal_qtd    = [by_mes[m]["qtd"]        for m in all_meses]
    mensal_val    = [round(by_mes[m]["valor"]) for m in all_meses]

    all_sk = list(mtd_s.keys())
    mtd_qtd_c  = sum(mtd_s[s]["cur"]["qtd"]  for s in all_sk)
    mtd_qtd_p  = sum(mtd_s[s]["prv"]["qtd"]  for s in all_sk)
    mtd_val_c  = sum(mtd_s[s]["cur"]["valor"] for s in all_sk)
    mtd_val_p  = sum(mtd_s[s]["prv"]["valor"] for s in all_sk)

    setor_order = [("public","Público"),("private","Private"),("grupo","Grupo"),("INSS","INSS")]
    s_labels, s_qtd = [], []
    for sk, slabel in setor_order:
        q = mtd_s[sk]["cur"]["qtd"]
        if q > 0: s_labels.append(slabel); s_qtd.append(q)

    def top5(ed, sk):
        items = [(e,d["qtd"]) for e,d in ed.items() if d["setor"]==sk and d["qtd"]>0]
        items.sort(key=lambda x:x[1],reverse=True)
        return [{"nome":e[:22],"qtd":q} for e,q in items[:5]]

    cards = {}
    for sk, slabel in [("public","public"),("private","private"),("grupo","grupo"),("INSS","inss")]:
        cd, pd = mtd_s[sk]["cur"], mtd_s[sk]["prv"]
        if cd["qtd"]==0 and pd["qtd"]==0: continue
        cards[slabel] = {
            "atual": cd["qtd"], "mm1": pd["qtd"],
            "valor_atual": round(cd["valor"]), "valor_mm1": round(pd["valor"]),
            "first_atual": prim_por_mes.get(cur_mes,0),
            "first_mm1":   prim_por_mes.get(prev_mes,0),
            "top": top5(mtd_ec, sk),
        }

    top10 = sorted(all_emp.items(), key=lambda x:x[1]["qtd"], reverse=True)[:10]
    empresas = []
    for i,(nome,d) in enumerate(top10,1):
        tk = round(d["valor"]/d["qtd"]) if d["qtd"] else 0
        empresas.append({"rank":i,"nome":nome[:30],"setor":d["setor"],"qtd":d["qtd"],"valor":round(d["valor"]),"ticket":tk,"first":0})

    return {
        "mtd_qtd_atual": mtd_qtd_c,  "mtd_qtd_mm1": mtd_qtd_p,
        "mtd_val_atual": round(mtd_val_c), "mtd_val_mm1": round(mtd_val_p),
        "mtd_first_atual": prim_por_mes.get(cur_mes,0),
        "mtd_first_mm1":   prim_por_mes.get(prev_mes,0),
        "mtd_users_atual": mtd_u_c, "mtd_users_mm1": mtd_u_p,
        "mensal_labels": mensal_labels, "mensal_qtd": mensal_qtd, "mensal_val": mensal_val,
        "setor_labels": s_labels, "setor_qtd": s_qtd,
        "mtd_por_setor": cards, "empresas": empresas,
    }

# ── Gerar HTML ────────────────────────────────────────────────────────────────
def generate_html(data):
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    now_str  = datetime.now().strftime("%d/%m/%Y %H:%M")
    html = (template
            .replace("%%DADOS%%",   json.dumps(data, ensure_ascii=False))
            .replace("'%%UPDATED%%';","'Atualizado "+now_str+"'; // gerado em "+now_str))
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"📄 {OUTPUT_PATH.name} ({OUTPUT_PATH.stat().st_size//1024} KB)")

# ── Git push ──────────────────────────────────────────────────────────────────
def git_push():
    now_str = datetime.now().strftime("%d/%m/%Y %H:%M")
    cmds = [
        ["git","-C",str(SCRIPT_DIR),"add","antecipacoes.html"],
        ["git","-C",str(SCRIPT_DIR),"commit","-m",f"chore: antecipacoes — {now_str}"],
        ["git","-C",str(SCRIPT_DIR),"push","origin","main"],
    ]
    for cmd in cmds:
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            if "nothing to commit" in r.stdout+r.stderr:
                print("   (sem mudanças)"); break
            print(f"   ⚠️  {r.stderr.strip()}")
        elif "push" in cmd:
            print(f"✅ {GITHUB_PAGES_URL}")

# ── Main ──────────────────────────────────────────────────────────────────────
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

    # Calcula ranges: 7 meses completos a partir do 1º dia de 7 meses atrás
    first_cur = date.today().replace(day=1)
    split     = first_cur - timedelta(days=1)          # último dia do mês anterior
    split     = split.replace(day=1)                   # 1º do mês anterior
    # range A: desde 7 meses atrás até o split
    inicio_a  = first_cur
    for _ in range(7): inicio_a = (inicio_a.replace(day=1) - timedelta(days=1)).replace(day=1)
    inicio_a_str = inicio_a.strftime("%Y-%m-%d")
    split_str    = split.strftime("%Y-%m-%d")
    # range B: split até amanhã
    fim_b_str    = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"\n[2/4] Queries Databricks ({inicio_a_str} → {fim_b_str})...")
    sid_a = submit(w, SQL_RANGE.format(inicio=inicio_a_str, fim=split_str))
    sid_b = submit(w, SQL_RANGE.format(inicio=split_str,    fim=fim_b_str))
    sid_p = submit(w, SQL_PRIM.format(inicio=inicio_a_str))
    print(f"   Submetidas: A={sid_a[:8]}... B={sid_b[:8]}... P={sid_p[:8]}...")

    rows_a = poll(w, sid_a, "Range A")
    rows_b = poll(w, sid_b, "Range B")
    rows_p = poll(w, sid_p, "Primeiras")
    rows   = rows_a + rows_b
    prim_por_mes = {r["mes"]: int(r["primeiras"]) for r in rows_p}
    print(f"   Total: {len(rows)} linhas | primeiras: {prim_por_mes}")

    print("\n[3/4] Transformando dados...")
    data = transform(rows, prim_por_mes)
    generate_html(data)

    print("\n[4/4] Publicando...")
    git_push()
    print(f"\n✅ Concluído — {datetime.now().strftime('%d/%m/%Y %H:%M')}")

if __name__ == "__main__":
    main()
