#!/usr/bin/env python3
"""
Relatório Antecipações de Salário — Atualização Diária
=======================================================
Uso:  python3 atualizar_antecipacoes.py

Lógica: mês vigente (MTD até hoje) vs mês anterior (mesmo nº de dias).
Duas queries paralelas: uma por mês. Rápido e preciso.

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

# ── datas ─────────────────────────────────────────────────────────────────────
def calc_datas():
    today    = date.today()
    cur_ini  = today.replace(day=1)
    prev_ini = (cur_ini - timedelta(days=1)).replace(day=1)
    # limite superior de cada mês
    cur_fim  = cur_ini.replace(month=cur_ini.month % 12 + 1) if cur_ini.month < 12 \
               else cur_ini.replace(year=cur_ini.year+1, month=1)
    prev_fim = cur_ini  # mês anterior termina no início do atual
    return {
        "cur_ini":  cur_ini.strftime("%Y-%m-%d"),
        "cur_fim":  cur_fim.strftime("%Y-%m-%d"),
        "prev_ini": prev_ini.strftime("%Y-%m-%d"),
        "prev_fim": prev_fim.strftime("%Y-%m-%d"),
        "cur_day":  today.day,
        "cur_mes":  cur_ini.strftime("%Y-%m"),
        "prev_mes": prev_ini.strftime("%Y-%m"),
        "cur_label":  cur_ini.strftime("%b/%y").capitalize(),   # ex: Mar/26
        "prev_label": prev_ini.strftime("%b/%y").capitalize(),  # ex: Fev/26
    }

# ── query por mês ─────────────────────────────────────────────────────────────
SQL_MES = """
SELECT
  CASE
    WHEN f.account_id = 1420                                     THEN 'INSS'
    WHEN f.account_id IN (6,52,57,58,60,62,65,73,100,3170,3675) THEN 'grupo'
    WHEN f.account_id IS NULL                                    THEN 'INSS'
    ELSE coalesce(e.flag_company_sector, 'private')
  END                                       AS setor,
  coalesce(upper(f.account_name), 'N/I')   AS secretaria,
  coalesce(upper(e.company_name), 'N/I')   AS empresa,
  count(*)                                 AS qtd,
  round(sum(cast(ar.request_value   AS double)), 2) AS valor_total,
  count(distinct ar.consumer_id)           AS usuarios_unicos
FROM benefits.anticipation_request ar
LEFT JOIN benefits.companies e ON ar.company_id = e.company_id
LEFT JOIN benefits.accounts  f ON e.account_id  = f.account_id
WHERE ar.request_status = 'FINISH'
  AND ar.created_at >= '{ini}'
  AND ar.created_at <  '{fim}'
  {filtro_dia}
GROUP BY 1,2,3
ORDER BY 1,3 DESC
"""

# primeiras antecipações no mês (consumers que anteciparam pela 1ª vez)
SQL_PRIM_MES = """
SELECT count(*) AS primeiras
FROM (
  SELECT consumer_id
  FROM benefits.anticipation_request
  WHERE request_status = 'FINISH'
  GROUP BY 1
  HAVING min(created_at) >= '{ini}'
     AND min(created_at) <  '{fim}'
     {filtro_dia_having}
) sub
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

def poll(w, sid, label="", timeout=300):
    from databricks.sdk.service.sql import StatementState
    elapsed = 0
    while elapsed < timeout:
        time.sleep(10); elapsed += 10
        s = w.statement_execution.get_statement(sid)
        st = s.status.state
        print(f"   [{elapsed:>3}s] {label} {st}     ", end="\r", flush=True)
        if st == StatementState.SUCCEEDED:
            print(f"\n   ✅ {label} ({elapsed}s)")
            cols = [c.name for c in s.manifest.schema.columns]
            return [dict(zip(cols, r)) for r in (s.result.data_array or [])]
        if st in (StatementState.FAILED, StatementState.CANCELED):
            raise RuntimeError(f"{label} falhou: {s.status.error.message if s.status.error else '?'}")
    raise TimeoutError(f"{label} timeout")

# ── transform ─────────────────────────────────────────────────────────────────
def agg_mes(rows):
    """Agrega linhas de um mês em totais por setor e top-5 empresas."""
    by_setor = defaultdict(lambda: {"qtd":0,"valor":0.0,"usuarios":0})
    by_emp   = defaultdict(lambda: {"setor":"","qtd":0,"valor":0.0})

    for r in rows:
        sk  = r["setor"]
        emp = r["empresa"]
        q   = int(r["qtd"]        or 0)
        v   = float(r["valor_total"] or 0)
        u   = int(r["usuarios_unicos"] or 0)

        by_setor[sk]["qtd"]    += q
        by_setor[sk]["valor"]  += v
        by_setor[sk]["usuarios"] += u
        by_emp[emp]["setor"]    = sk
        by_emp[emp]["qtd"]     += q
        by_emp[emp]["valor"]   += v

    # top-5 por setor
    def top5(sk):
        items = [(e,d["qtd"]) for e,d in by_emp.items() if d["setor"]==sk and d["qtd"]>0]
        items.sort(key=lambda x:x[1], reverse=True)
        return [{"nome":e[:22],"qtd":q} for e,q in items[:5]]

    result = {}
    for sk, d in by_setor.items():
        result[sk] = {**d, "top": top5(sk)}
    return result, by_emp

def build_data(cur_rows, prev_rows, prim_cur, prim_prev, datas):
    cur_agg,  cur_emp  = agg_mes(cur_rows)
    prev_agg, prev_emp = agg_mes(prev_rows)

    ORDEM = ['public','grupo','private','INSS']
    LABELS = {'public':'Público','private':'Private','grupo':'Grupo','INSS':'INSS'}

    # totais gerais
    def tot(agg, field): return sum(d[field] for d in agg.values())

    tot_qtd_c = tot(cur_agg,  "qtd");   tot_qtd_p = tot(prev_agg, "qtd")
    tot_val_c = tot(cur_agg,  "valor"); tot_val_p = tot(prev_agg, "valor")
    tot_usr_c = tot(cur_agg,  "usuarios"); tot_usr_p = tot(prev_agg, "usuarios")
    tk_c = round(tot_val_c/tot_qtd_c) if tot_qtd_c else 0
    tk_p = round(tot_val_p/tot_qtd_p) if tot_qtd_p else 0

    # mtd_por_setor
    setores_presentes = sorted(set(list(cur_agg.keys())+list(prev_agg.keys())),
                                key=lambda x: ORDEM.index(x) if x in ORDEM else 99)
    mtd_por_setor = {}
    for sk in setores_presentes:
        cd = cur_agg.get(sk,  {"qtd":0,"valor":0.0,"top":[]})
        pd = prev_agg.get(sk, {"qtd":0,"valor":0.0,"top":[]})
        slabel = sk.lower() if sk not in ('INSS',) else 'inss'
        mtd_por_setor[slabel] = {
            "atual":       cd["qtd"],
            "mm1":         pd["qtd"],
            "valor_atual": round(cd["valor"]),
            "valor_mm1":   round(pd["valor"]),
            "top":         cd.get("top", []),
        }

    # donut (setor cur)
    donut_labels, donut_qtd = [], []
    for sk in ORDEM:
        q = cur_agg.get(sk,{}).get("qtd",0)
        if q > 0:
            donut_labels.append(LABELS.get(sk, sk))
            donut_qtd.append(q)

    # top-10 empresas geral (mês atual)
    top10 = sorted(cur_emp.items(), key=lambda x:x[1]["qtd"], reverse=True)[:10]
    empresas = []
    for i,(nome,d) in enumerate(top10,1):
        tk = round(d["valor"]/d["qtd"]) if d["qtd"] else 0
        empresas.append({"rank":i,"nome":nome[:30],"setor":d["setor"],
                         "qtd":d["qtd"],"valor":round(d["valor"]),"ticket":tk})

    return {
        # hero
        "cur_label":  datas["cur_label"],
        "prev_label": datas["prev_label"],
        "mtd_qtd_atual":   tot_qtd_c,   "mtd_qtd_mm1":   tot_qtd_p,
        "mtd_val_atual":   round(tot_val_c), "mtd_val_mm1": round(tot_val_p),
        "mtd_users_atual": tot_usr_c,   "mtd_users_mm1": tot_usr_p,
        "mtd_first_atual": prim_cur,    "mtd_first_mm1": prim_prev,
        # setor
        "setor_labels": donut_labels, "setor_qtd": donut_qtd,
        "mtd_por_setor": mtd_por_setor,
        # tabela
        "empresas": empresas,
    }

# ── html ──────────────────────────────────────────────────────────────────────
def generate_html(data, datas):
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    now_str  = datetime.now().strftime("%d/%m/%Y %H:%M")
    periodo  = f"{datas['cur_label']} vs {datas['prev_label']}"
    html = (template
            .replace("%%DADOS%%",   json.dumps(data, ensure_ascii=False))
            .replace("%%UPDATED%%", now_str)
            .replace("%%PERIODO%%", periodo))
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"📄 {OUTPUT_PATH.name} ({OUTPUT_PATH.stat().st_size//1024} KB)")

# ── git push ──────────────────────────────────────────────────────────────────
def git_push():
    now_str = datetime.now().strftime("%d/%m/%Y %H:%M")
    for cmd in [
        ["git","-C",str(SCRIPT_DIR),"add","antecipacoes.html"],
        ["git","-C",str(SCRIPT_DIR),"commit","-m",f"chore: antecipacoes — {now_str}"],
        ["git","-C",str(SCRIPT_DIR),"push","origin","main"],
    ]:
        r = subprocess.run(cmd, capture_output=True, text=True)
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

    datas = calc_datas()
    print(f"\n[2/4] Período: {datas['cur_label']} (até dia {datas['cur_day']}) vs {datas['prev_label']} (mesmo período)")
    print(f"      cur:  {datas['cur_ini']} → hoje")
    print(f"      prev: {datas['prev_ini']} → dia {datas['cur_day']}")

    # filtro de dia: mês atual = até hoje; mês anterior = mesmo dia
    filtro_cur  = f"AND day(ar.created_at) <= {datas['cur_day']}"
    filtro_prev = f"AND day(ar.created_at) <= {datas['cur_day']}"

    sid_cur  = submit(w, SQL_MES.format(ini=datas["cur_ini"],  fim=datas["cur_fim"],  filtro_dia=filtro_cur))
    sid_prev = submit(w, SQL_MES.format(ini=datas["prev_ini"], fim=datas["prev_fim"], filtro_dia=filtro_prev))
    sid_pc   = submit(w, SQL_PRIM_MES.format(ini=datas["cur_ini"],  fim=datas["cur_fim"],
                                              filtro_dia_having=f"AND day(min(created_at)) <= {datas['cur_day']}"))
    sid_pp   = submit(w, SQL_PRIM_MES.format(ini=datas["prev_ini"], fim=datas["prev_fim"],
                                              filtro_dia_having=f"AND day(min(created_at)) <= {datas['cur_day']}"))
    print(f"   4 queries submetidas em paralelo")

    cur_rows  = poll(w, sid_cur,  f"{datas['cur_label']}")
    prev_rows = poll(w, sid_prev, f"{datas['prev_label']}")
    prim_cur  = int((poll(w, sid_pc, "Primeiras cur") [0] or {}).get("primeiras", 0) or 0)
    prim_prev = int((poll(w, sid_pp, "Primeiras prev")[0] or {}).get("primeiras", 0) or 0)

    print(f"\n[3/4] Transformando dados...")
    data = build_data(cur_rows, prev_rows, prim_cur, prim_prev, datas)
    print(f"   MTD {datas['cur_label']}: {data['mtd_qtd_atual']:,} antecip | R$ {data['mtd_val_atual']:,.0f}")
    print(f"   MTD {datas['prev_label']}: {data['mtd_qtd_mm1']:,} antecip | R$ {data['mtd_val_mm1']:,.0f}")
    generate_html(data, datas)

    print("\n[4/4] Publicando...")
    git_push()
    print(f"\n✅ Concluído — {datetime.now().strftime('%d/%m/%Y %H:%M')}")

if __name__ == "__main__":
    main()
