#!/usr/bin/env python3
"""
Score Antecipações — Atualização Diária
=======================================
Busca todos os dados do mês corrente no Databricks e atualiza o score.html.
Uso: python3 atualizar_score.py
"""

import json, time, sys, warnings, subprocess, re
from datetime import datetime, date
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore")

DATABRICKS_HOST    = "https://picpay-principal.cloud.databricks.com"
DATABRICKS_PROFILE = "picpay"
WAREHOUSE_ID       = "6077a99f149e0d70"
SCRIPT_DIR         = Path(__file__).parent
SCORE_PATH         = Path.home() / "score-antecipacoes" / "index.html"

MES_ATUAL = date.today().strftime("%Y-%m")
_MESES_PT = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]
MES_LABEL = f"{_MESES_PT[date.today().month - 1]}/{date.today().strftime('%y')}"

print(f"[{datetime.now().strftime('%H:%M:%S')}] Iniciando atualização Score — {MES_ATUAL}")

# ── Auth ───────────────────────────────────────────────────────────
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState
except ImportError:
    print("ERRO: databricks-sdk não instalado. Rode: pip install databricks-sdk")
    sys.exit(1)

w = WorkspaceClient(host=DATABRICKS_HOST, profile=DATABRICKS_PROFILE)

def run_q(sql, timeout=300, label="query"):
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=sql, wait_timeout="0s"
    )
    sid = resp.statement_id
    for _ in range(timeout // 5):
        time.sleep(5)
        s = w.statement_execution.get_statement(sid)
        if s.status.state == StatementState.SUCCEEDED:
            cols = [c.name for c in s.manifest.schema.columns]
            rows = s.result.data_array or []
            print(f"  ✅ {label}: {len(rows)} rows")
            return cols, [list(r) for r in rows]
        elif s.status.state in (StatementState.FAILED, StatementState.CANCELED):
            raise Exception(s.status.error.message if s.status.error else "falhou")
    raise Exception(f"timeout ({timeout}s)")

SETOR_CASE = """CASE
    WHEN f.account_id = 1420 OR f.account_id IS NULL THEN 'INSS'
    WHEN f.account_id IN (6,52,57,58,60,62,65,73,100,3170,3675) THEN 'Grupo'
    WHEN co2.flag_company_sector = 'public' THEN 'Publico'
    ELSE 'Privado'
  END"""

# ── Queries em paralelo ────────────────────────────────────────────
queries = {
    "funil": f"""
WITH vidas AS (
  SELECT co.consumer_id, co.collaborator_id
  FROM benefits.collaborators co
  JOIN benefits.companies co2 ON co.company_id=co2.company_id
  LEFT JOIN benefits.accounts f ON co2.account_id=f.account_id
  WHERE co.resignation_date IS NULL AND co.consumer_id IS NOT NULL
),
margem AS (
  SELECT DISTINCT collaborator_id FROM benefits.sec_wage_advance_collaborator_margins
  WHERE is_enabled=true AND resignation_date IS NULL
),
mau30 AS (
  SELECT consumer_id FROM consumers.daily_consumers_labels_and_metrics
  WHERE metric_date=date_sub(current_date(),7) AND is_mau_app_30=true
),
mau_geral AS (
  SELECT consumer_id FROM consumers.daily_consumers_labels_and_metrics
  WHERE metric_date=date_sub(current_date(),7) AND is_mau_geral=true
),
antecip AS (
  -- Igual ao TV: todos que anteciparam no mês, sem filtro de colaborador ativo
  SELECT DISTINCT consumer_id FROM benefits.anticipation_request
  WHERE request_status='FINISH' AND date_format(created_at,'yyyy-MM')='{MES_ATUAL}'
)
SELECT
  COUNT(DISTINCT v.consumer_id)                                                              AS vidas,
  COUNT(DISTINCT CASE WHEN m.collaborator_id IS NOT NULL THEN v.consumer_id END)             AS com_margem,
  COUNT(DISTINCT CASE WHEN m.collaborator_id IS NULL     THEN v.consumer_id END)             AS sem_margem,
  COUNT(DISTINCT mau30.consumer_id)                                                           AS mau_30,
  COUNT(DISTINCT CASE WHEN mau30.consumer_id IS NOT NULL AND m.collaborator_id IS NOT NULL THEN v.consumer_id END) AS mau_30_margem,
  COUNT(DISTINCT mau_geral.consumer_id)                                                       AS mau_geral,
  COUNT(DISTINCT CASE WHEN mau_geral.consumer_id IS NOT NULL AND m.collaborator_id IS NOT NULL THEN v.consumer_id END) AS mau_geral_margem,
  -- antecipando: total direto da anticipation_request (igual ao TV, sem interseção com vidas ativas)
  (SELECT COUNT(DISTINCT consumer_id) FROM antecip)                                           AS antecipando
FROM vidas v
LEFT JOIN margem m      ON v.collaborator_id=m.collaborator_id
LEFT JOIN mau30         ON v.consumer_id=mau30.consumer_id
LEFT JOIN mau_geral     ON v.consumer_id=mau_geral.consumer_id
""",

    "total_vidas": """
SELECT
  COUNT(*) AS total_vidas,
  COUNT(CASE WHEN consumer_id IS NOT NULL THEN 1 END) AS com_conta,
  COUNT(CASE WHEN consumer_id IS NULL THEN 1 END) AS sem_conta
FROM benefits.collaborators WHERE resignation_date IS NULL
""",

    "setores": f"""
WITH bacen AS (
  -- Consumer mais recente validado pelo BACEN por CPF
  -- Garante com_conta <= total_vidas (sem dupla contagem de reaberturas de conta)
  SELECT cpf, consumer_id
  FROM (
    SELECT cpf, consumer_id,
           ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY sent_bacen_at DESC) AS rn
    FROM consumers.consumers
    WHERE sent_bacen_at IS NOT NULL AND cpf IS NOT NULL
  ) WHERE rn = 1
),
base AS (
  -- Todos os colaboradores ativos, com ou sem conta
  SELECT co.collaborator_id, co.document_number, {SETOR_CASE} AS setor
  FROM benefits.collaborators co
  JOIN benefits.companies co2 ON co.company_id=co2.company_id
  LEFT JOIN benefits.accounts f ON co2.account_id=f.account_id
  WHERE co.resignation_date IS NULL
),
conta AS (
  -- Join pelo CPF: pega o consumer_id mais recente pelo BACEN
  SELECT b.collaborator_id, b.setor, bk.consumer_id AS consumer_bacen
  FROM base b
  LEFT JOIN bacen bk ON b.document_number = bk.cpf
),
margem AS (
  SELECT DISTINCT collaborator_id FROM benefits.sec_wage_advance_collaborator_margins
  WHERE is_enabled=true AND resignation_date IS NULL
),
mau_geral AS (
  SELECT consumer_id FROM consumers.daily_consumers_labels_and_metrics
  WHERE metric_date=date_sub(current_date(),7) AND is_mau_geral=true
),
antecip_setor AS (
  SELECT
    CASE
      WHEN f.account_id = 1420 OR f.account_id IS NULL THEN 'INSS'
      WHEN f.account_id IN (6,52,57,58,60,62,65,73,100,3170,3675) THEN 'Grupo'
      WHEN co2.flag_company_sector = 'public' THEN 'Publico'
      ELSE 'Privado'
    END AS setor,
    COUNT(DISTINCT ar.consumer_id) AS antecipando
  FROM benefits.anticipation_request ar
  LEFT JOIN benefits.companies co2 ON ar.company_id=co2.company_id
  LEFT JOIN benefits.accounts f ON co2.account_id=f.account_id
  WHERE ar.request_status='FINISH' AND date_format(ar.created_at,'yyyy-MM')='{MES_ATUAL}'
  GROUP BY 1
)
SELECT c.setor,
  COUNT(DISTINCT c.collaborator_id)                                                                                 AS total_vidas,
  COUNT(DISTINCT c.consumer_bacen)                                                                                  AS com_conta,
  COUNT(DISTINCT c.collaborator_id) - COUNT(DISTINCT c.consumer_bacen)                                             AS sem_conta,
  COUNT(DISTINCT CASE WHEN m.collaborator_id IS NOT NULL THEN c.consumer_bacen END)                                 AS com_margem,
  COUNT(DISTINCT CASE WHEN m.collaborator_id IS NULL     THEN c.consumer_bacen END)                                 AS sem_margem,
  COUNT(DISTINCT mau.consumer_id)                                                                                   AS mau_geral,
  COUNT(DISTINCT CASE WHEN mau.consumer_id IS NOT NULL AND m.collaborator_id IS NOT NULL THEN c.consumer_bacen END) AS mau_com_margem,
  COUNT(DISTINCT CASE WHEN mau.consumer_id IS NOT NULL AND m.collaborator_id IS NULL     THEN c.consumer_bacen END) AS mau_sem_margem,
  COALESCE(ast.antecipando, 0)                                                                                      AS antecipando
FROM conta c
LEFT JOIN margem m      ON c.collaborator_id = m.collaborator_id
LEFT JOIN mau_geral mau ON c.consumer_bacen  = mau.consumer_id
LEFT JOIN antecip_setor ast ON c.setor       = ast.setor
GROUP BY c.setor, ast.antecipando
ORDER BY total_vidas DESC
""",

    "historico": """
SELECT date_format(created_at,'yyyy-MM') AS mes,
  COUNT(DISTINCT consumer_id) AS consumers,
  COUNT(*) AS antecipacoes,
  ROUND(SUM(request_value)/1e6,1) AS tpv_mi,
  ROUND(AVG(request_value),0) AS ticket
FROM benefits.anticipation_request
WHERE request_status='FINISH'
  AND created_at >= add_months(date_trunc('month',current_date()),-12)
GROUP BY 1 ORDER BY 1
""",

    "rfm": """
SELECT
  CASE WHEN cnt=1 THEN '1x (só uma vez)' WHEN cnt<=3 THEN '2-3x'
       WHEN cnt<=6 THEN '4-6x' WHEN cnt<=12 THEN '7-12x' ELSE '13x+' END AS faixa,
  COUNT(DISTINCT consumer_id) AS consumers,
  ROUND(AVG(cnt),1) AS media_freq
FROM (
  SELECT consumer_id, COUNT(*) AS cnt
  FROM benefits.anticipation_request
  WHERE request_status='FINISH' AND created_at >= add_months(current_date(),-12)
  GROUP BY consumer_id
) t GROUP BY 1 ORDER BY MIN(cnt)
""",

    "dist_valor": """
SELECT
  CASE WHEN request_value<200 THEN 'Até R$200' WHEN request_value<500 THEN 'R$200-500'
       WHEN request_value<1000 THEN 'R$500-1k' WHEN request_value<2000 THEN 'R$1k-2k'
       WHEN request_value<5000 THEN 'R$2k-5k' ELSE 'R$5k+' END AS faixa,
  COUNT(*) AS antecipacoes,
  COUNT(DISTINCT consumer_id) AS consumers,
  ROUND(AVG(request_value),2) AS ticket_medio
FROM benefits.anticipation_request
WHERE request_status='FINISH' AND created_at >= add_months(current_date(),-12)
GROUP BY 1 ORDER BY MIN(request_value)
""",

    "cashin": """
SELECT date_format(created_at,'yyyy-MM') AS mes, cashin_destiny,
  ROUND(SUM(request_value)/1e6,2) AS tpv_mi
FROM benefits.anticipation_request
WHERE request_status='FINISH'
  AND created_at >= add_months(date_trunc('month',current_date()),-13)
  AND cashin_destiny IS NOT NULL
GROUP BY 1,2 ORDER BY 1,3 DESC
""",

    "recencia": """
SELECT
  CASE WHEN d<=30 THEN '0-30d' WHEN d<=60 THEN '31-60d' WHEN d<=90 THEN '61-90d'
       WHEN d<=180 THEN '91-180d' WHEN d<=365 THEN '181-365d' ELSE '365d+' END AS faixa,
  COUNT(DISTINCT consumer_id) AS consumers,
  ROUND(AVG(pct_mau)*100,1) AS pct_mau,
  ROUND(AVG(ticket),2) AS ticket_medio
FROM (
  SELECT consumer_id,
    datediff(current_date(), max(date(created_at))) AS d,
    COUNT(*)/12.0 AS pct_mau,
    AVG(request_value) AS ticket
  FROM benefits.anticipation_request
  WHERE request_status='FINISH' AND created_at >= add_months(current_date(),-12)
  GROUP BY consumer_id
) t GROUP BY 1 ORDER BY MIN(d)
"""
}

# Rodar em paralelo
results = {}
with ThreadPoolExecutor(max_workers=4) as ex:
    futures = {ex.submit(run_q, sql, 300, name): name for name, sql in queries.items()}
    for fut in as_completed(futures):
        name = futures[fut]
        try:
            cols, rows = fut.result()
            results[name] = {"cols": cols, "rows": rows}
        except Exception as e:
            print(f"  ❌ {name}: {e}")
            results[name] = None

# ── Processar resultados ───────────────────────────────────────────
def r2d(name, col):
    """Pegar valor de uma query de 1 linha pelo nome da coluna."""
    res = results.get(name)
    if not res or not res["rows"]: return 0
    idx = res["cols"].index(col) if col in res["cols"] else -1
    return int(res["rows"][0][idx] or 0) if idx >= 0 else 0

def fmt_k(n):
    if n >= 1_000_000: return f"{n/1_000_000:.2f}M"
    if n >= 1_000:     return f"{n/1_000:.1f}k"
    return str(n)

# Funil
vidas       = r2d("funil", "vidas")
com_margem  = r2d("funil", "com_margem")
sem_margem  = r2d("funil", "sem_margem")
mau_30      = r2d("funil", "mau_30")
mau_30_m    = r2d("funil", "mau_30_margem")
mau_geral   = r2d("funil", "mau_geral")
mau_geral_m = r2d("funil", "mau_geral_margem")
antecipando = r2d("funil", "antecipando")
total_v     = r2d("total_vidas", "total_vidas")
com_conta   = r2d("total_vidas", "com_conta")
sem_conta   = r2d("total_vidas", "sem_conta")
pct_conta   = round(com_conta/total_v*100) if total_v else 0

# Setores
setor_data = {}
if results.get("setores") and results["setores"]["rows"]:
    for row in results["setores"]["rows"]:
        d = dict(zip(results["setores"]["cols"], row))
        setor_data[d["setor"]] = d

# Histórico
hist_meses, hist_ants, hist_cons, hist_tpv, hist_ticket = [], [], [], [], []
if results.get("historico") and results["historico"]["rows"]:
    for row in results["historico"]["rows"]:
        d = dict(zip(results["historico"]["cols"], row))
        hist_meses.append(d["mes"])
        hist_ants.append(int(d["antecipacoes"] or 0))
        hist_cons.append(int(d["consumers"] or 0))
        hist_tpv.append(float(d["tpv_mi"] or 0))
        hist_ticket.append(int(d["ticket"] or 0))

# RFM
rfm_rows = []
if results.get("rfm") and results["rfm"]["rows"]:
    for row in results["rfm"]["rows"]:
        d = dict(zip(results["rfm"]["cols"], row))
        rfm_rows.append([d["faixa"], int(d["consumers"] or 0), float(d["media_freq"] or 0)])

# Dist valor
dist_rows = []
if results.get("dist_valor") and results["dist_valor"]["rows"]:
    for row in results["dist_valor"]["rows"]:
        d = dict(zip(results["dist_valor"]["cols"], row))
        dist_rows.append([d["faixa"], int(d["antecipacoes"] or 0), int(d["consumers"] or 0), float(d["ticket_medio"] or 0)])

# Cashin
cashin_meses = []
cashin_series = {"WALLETS": [], "PIX": [], "BENEFITS": [], "INSS_CARD": []}
if results.get("cashin") and results["cashin"]["rows"]:
    meses_set = sorted(set(r[0] for r in results["cashin"]["rows"]))
    cashin_meses = meses_set
    data_map = {}
    for row in results["cashin"]["rows"]:
        mes, dest, tpv = row[0], row[1], float(row[2] or 0)
        if mes not in data_map: data_map[mes] = {}
        data_map[mes][dest] = tpv
    for dest in cashin_series:
        cashin_series[dest] = [round(data_map.get(m, {}).get(dest, 0), 2) for m in meses_set]

# Recência
rec_rows = []
if results.get("recencia") and results["recencia"]["rows"]:
    for row in results["recencia"]["rows"]:
        d = dict(zip(results["recencia"]["cols"], row))
        rec_rows.append([d["faixa"], int(d["consumers"] or 0), float(d["pct_mau"] or 0), float(d["ticket_medio"] or 0)])

# ── Construir novo DATA JS ─────────────────────────────────────────
pub = setor_data.get("Publico", {})
grp = setor_data.get("Grupo",   {})
prv = setor_data.get("Privado", {})

new_data = {
    "funil": {
        "vidas":        total_v,
        "contas":       com_conta,
        "com_margem":   com_margem,
        "sem_margem":   sem_margem,
        "sem_conta":    sem_conta,
        "mau":          mau_30,
        "mau_margem":   mau_30_m,
        "antecipando":  antecipando
    },
    "setores": {
        "public": {
            "nome": "Público", "cor": "#58a6ff",
            "vidas":      int(pub.get("total_vidas", 0)),
            "contas":     int(pub.get("com_conta",   0)),
            "com_margem": int(pub.get("com_margem",  0)),
            "sem_margem": int(pub.get("sem_margem",  0)),
            "sem_conta":  int(pub.get("sem_conta",   0)),
            "mau":        int(pub.get("mau_geral",   0)),
            "mau_margem": int(pub.get("mau_com_margem", 0)),
            "antecipando":int(pub.get("antecipando", 0))
        },
        "grupo": {
            "nome": "Grupo", "cor": "#bc8cff",
            "vidas":      int(grp.get("total_vidas", 0)),
            "contas":     int(grp.get("com_conta",   0)),
            "com_margem": int(grp.get("com_margem",  0)),
            "sem_margem": int(grp.get("sem_margem",  0)),
            "sem_conta":  int(grp.get("sem_conta",   0)),
            "mau":        int(grp.get("mau_geral",   0)),
            "mau_margem": int(grp.get("mau_com_margem", 0)),
            "antecipando":int(grp.get("antecipando", 0))
        },
        "private": {
            "nome": "Privado", "cor": "#f0883e",
            "vidas":      int(prv.get("total_vidas", 0)),
            "contas":     int(prv.get("com_conta",   0)),
            "com_margem": int(prv.get("com_margem",  0)),
            "sem_margem": int(prv.get("sem_margem",  0)),
            "sem_conta":  int(prv.get("sem_conta",   0)),
            "mau":        int(prv.get("mau_geral",   0)),
            "mau_margem": int(prv.get("mau_com_margem", 0)),
            "antecipando":int(prv.get("antecipando", 0))
        }
    },
    "historico": {
        "meses":        hist_meses,
        "antecipacoes": hist_ants,
        "consumers":    hist_cons,
        "tpv":          hist_tpv,
        "ticket":       hist_ticket
    },
    "freq_dist":  rfm_rows,
    "recencia":   rec_rows,
    "dist_valor": dist_rows,
    "cashin": {
        "meses":  cashin_meses,
        "series": cashin_series
    }
}

# ── Atualizar HTML ─────────────────────────────────────────────────
print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Atualizando HTML...")

with open(SCORE_PATH) as f:
    html = f.read()

# Substituir o bloco DATA JS completo
import re as re_mod

new_data_js = f"const DATA = {json.dumps(new_data, ensure_ascii=False)};"
html = re_mod.sub(r'const DATA = \{.*?\};', new_data_js, html, flags=re_mod.DOTALL)

# Atualizar SETOR_DATA JS (card único)
def setor_card_vals(s_dict, total_vidas_str, pct_ativ):
    conta   = int(s_dict.get("com_conta",  0))
    margem  = int(s_dict.get("com_margem", 0))
    sem_m   = int(s_dict.get("sem_margem", 0))
    mau_g   = int(s_dict.get("mau_geral",  0))
    mau_cm  = int(s_dict.get("mau_com_margem", 0))
    mau_sm  = int(s_dict.get("mau_sem_margem", 0))
    ant     = int(s_dict.get("antecipando", 0))
    atv     = round(conta/int(total_vidas_str.replace('.','').replace('M','000000').replace('k','000')) * 100) if conta else pct_ativ
    cmg_pct = round(margem/conta*100) if conta else 0
    mau_pct = round(mau_g/conta*100)  if conta else 0
    ant_pct = round(ant/mau_cm*100)   if mau_cm else 0
    return conta, margem, sem_m, mau_g, mau_cm, mau_sm, ant, atv, cmg_pct, mau_pct, ant_pct

# Atualizar SETOR_DATA total
antecipando_total = antecipando
ant_pct_total = round(antecipando/mau_geral_m*100) if mau_geral_m else 0
mau_sem_m = mau_geral - mau_geral_m

# Helper pct seguro
def _pct(a, b): return round(a/b*100) if b else 0

# Valores de setor vindos da query ao vivo (sem hardcoded)
pub_tv = int(pub.get("total_vidas", 0)); pub_cc = int(pub.get("com_conta", 0)); pub_sc = int(pub.get("sem_conta", 0))
grp_tv = int(grp.get("total_vidas", 0)); grp_cc = int(grp.get("com_conta", 0)); grp_sc = int(grp.get("sem_conta", 0))
prv_tv = int(prv.get("total_vidas", 0)); prv_cc = int(prv.get("com_conta", 0)); prv_sc = int(prv.get("sem_conta", 0))

setor_data_new = f"""var SETOR_DATA = {{
  total: {{
    label:'Total', icon:'📊', cor:'var(--green)',
    vidas:'{fmt_k(total_v)}', conta:'{fmt_k(com_conta)}', semConta:'{fmt_k(sem_conta)}', pctConta:'{pct_conta}%',
    comMargem:'{fmt_k(com_margem)}', semMargem:'{fmt_k(sem_margem)}',
    mau:'{fmt_k(mau_geral)}', mauComMargem:'{fmt_k(mau_geral_m)}', mauSemMargem:'{fmt_k(mau_sem_m)}',
    antecipando:'{fmt_k(antecipando)}',
    atv:{pct_conta}, cmg:{_pct(com_margem,com_conta)},
    mauPct:{_pct(mau_geral,com_conta)}, ant:{ant_pct_total}
  }},
  publico: {{
    label:'Público', icon:'🏛️', cor:'var(--blue)',
    vidas:'{fmt_k(pub_tv)}', conta:'{fmt_k(pub_cc)}', semConta:'{fmt_k(pub_sc)}', pctConta:'{_pct(pub_cc,pub_tv)}%',
    comMargem:'{fmt_k(int(pub.get("com_margem",0)))}', semMargem:'{fmt_k(int(pub.get("sem_margem",0)))}',
    mau:'{fmt_k(int(pub.get("mau_geral",0)))}', mauComMargem:'{fmt_k(int(pub.get("mau_com_margem",0)))}', mauSemMargem:'{fmt_k(int(pub.get("mau_sem_margem",0)))}',
    antecipando:'{fmt_k(int(pub.get("antecipando",0)))}',
    atv:{_pct(pub_cc,pub_tv)}, cmg:{_pct(int(pub.get("com_margem",0)),pub_cc)},
    mauPct:{_pct(int(pub.get("mau_geral",0)),pub_cc)}, ant:{_pct(int(pub.get("antecipando",0)),int(pub.get("mau_com_margem",1)))}
  }},
  grupo: {{
    label:'Grupo', icon:'🏗️', cor:'var(--purple)',
    vidas:'{fmt_k(grp_tv)}', conta:'{fmt_k(grp_cc)}', semConta:'{fmt_k(grp_sc)}', pctConta:'{_pct(grp_cc,grp_tv)}%',
    comMargem:'{fmt_k(int(grp.get("com_margem",0)))}', semMargem:'{fmt_k(int(grp.get("sem_margem",0)))}',
    mau:'{fmt_k(int(grp.get("mau_geral",0)))}', mauComMargem:'{fmt_k(int(grp.get("mau_com_margem",0)))}', mauSemMargem:'{fmt_k(int(grp.get("mau_sem_margem",0)))}',
    antecipando:'{fmt_k(int(grp.get("antecipando",0)))}',
    atv:{_pct(grp_cc,grp_tv)}, cmg:{_pct(int(grp.get("com_margem",0)),grp_cc)},
    mauPct:{_pct(int(grp.get("mau_geral",0)),grp_cc)}, ant:{_pct(int(grp.get("antecipando",0)),int(grp.get("mau_com_margem",1)))}
  }},
  privado: {{
    label:'Privado', icon:'🏭', cor:'var(--orange)',
    vidas:'{fmt_k(prv_tv)}', conta:'{fmt_k(prv_cc)}', semConta:'{fmt_k(prv_sc)}', pctConta:'{_pct(prv_cc,prv_tv)}%',
    comMargem:'{fmt_k(int(prv.get("com_margem",0)))}', semMargem:'{fmt_k(int(prv.get("sem_margem",0)))}',
    mau:'{fmt_k(int(prv.get("mau_geral",0)))}', mauComMargem:'{fmt_k(int(prv.get("mau_com_margem",0)))}', mauSemMargem:'{fmt_k(int(prv.get("mau_sem_margem",0)))}',
    antecipando:'{fmt_k(int(prv.get("antecipando",0)))}',
    atv:{_pct(prv_cc,prv_tv)}, cmg:{_pct(int(prv.get("com_margem",0)),prv_cc)},
    mauPct:{_pct(int(prv.get("mau_geral",0)),prv_cc)}, ant:{_pct(int(prv.get("antecipando",0)),int(prv.get("mau_com_margem",1))) if int(prv.get("mau_com_margem",0)) else 0}
  }}
}};"""

html = re_mod.sub(r'var SETOR_DATA = \{.*?\};', setor_data_new, html, flags=re_mod.DOTALL)

# ── Atualizar valores hardcoded dos KPI cards (Total) ─────────────
# Garante que o HTML inicial já mostra os valores corretos antes do JS rodar
pct_sem_conta  = round(sem_conta/total_v*100)  if total_v  else 0
pct_sem_margem = round(sem_margem/com_conta*100) if com_conta else 0
pct_mau        = round(mau_geral/com_conta*100)  if com_conta else 0
pct_mau_sm     = round(mau_sem_m/com_conta*100)  if com_conta else 0

kpi_updates = {
    r'(id="kpi-vidas">)[^<]+'        : f'\\g<1>{fmt_k(total_v)}',
    r'(id="kpi-vidas-sub">)[^<]+'    : r'\g<1>Elegíveis ao produto',
    r'(id="kpi-conta">)[^<]+'        : f'\\g<1>{fmt_k(com_conta)}',
    r'(id="kpi-conta-sub">)[^<]+'    : f'\\g<1>{pct_conta}% das vidas',
    r'(id="kpi-margem">)[^<]+'       : f'\\g<1>{fmt_k(com_margem)}',
    r'(id="kpi-margem-sub">)[^<]+'   : f'\\g<1>{round(com_margem/com_conta*100) if com_conta else 0}% dos com conta',
    r'(id="kpi-mau">)[^<]+'          : f'\\g<1>{fmt_k(mau_geral)}',
    r'(id="kpi-mau-sub">)[^<]+'      : f'\\g<1>{pct_mau}% dos com conta',
    r'(id="kpi-semmargem">)[^<]+'    : f'\\g<1>{fmt_k(sem_margem)}',
    r'(id="kpi-semmargem-sub">)[^<]+': f'\\g<1>{pct_sem_margem}% dos com conta',
    r'(id="kpi-ant">)[^<]+'          : f'\\g<1>{fmt_k(antecipando)}',
    r'(id="kpi-ant-sub">)[^<]+'      : f'\\g<1>{ant_pct_total}% MAU c/ margem',
    r'(id="kpi-semconta">)[^<]+'     : f'\\g<1>{fmt_k(sem_conta)}',
    r'(id="kpi-semconta-sub">)[^<]+' : f'\\g<1>{pct_sem_conta}% sem conta ainda',
    r'(id="kpi-mausemmargem">)[^<]+' : f'\\g<1>{fmt_k(mau_sem_m)}',
}
for pattern, repl in kpi_updates.items():
    html = re_mod.sub(pattern, repl, html)

# Atualizar hero tags
mes_str = MES_LABEL
html = re_mod.sub(r'⚡ [\d,\.]+k antecipando em \w+/\d+',
                  f'⚡ {fmt_k(antecipando).replace(".",",")} antecipando em {mes_str}', html)
html = re_mod.sub(r'📱 [\d,\.]+k MAU 30d',
                  f'📱 {fmt_k(mau_30).replace(".",",")} MAU 30d', html)
html = re_mod.sub(r'💳 [\d,\.]+k com conta \(\d+%\)',
                  f'💳 {fmt_k(com_conta).replace(".",",")} com conta ({pct_conta}%)', html)

# ── Atualizar funil visual (fn2-val, fn2-drop, sec-sub) ────────────────
# Valores derivados para os drops entre etapas
sem_conta_funil   = total_v - com_conta          # vidas sem conta
sem_margem_funil  = sem_margem                   # com conta mas sem margem
mau_sem_margem_f  = com_margem - mau_geral_m     # com margem mas não MAU
nao_anteciparam   = mau_geral_m - antecipando    # MAU+margem que não anteciparam
pct_ant_mau       = round(antecipando / mau_geral_m * 100) if mau_geral_m else 0

def fk(n):  # formata como "528,7k" ou "1,35M"
    return fmt_k(n).replace(".", ",")

# sec-sub do funil: "Do total de X vidas elegíveis até as Yk que anteciparam em Mês/AA."
html = re_mod.sub(
    r'Do total de [\d,\.]+[kM] vidas elegíveis até as [\d,\.]+[kM] que anteciparam em \w+/\d+\.',
    f'Do total de {fk(total_v)} vidas elegíveis até as {fk(antecipando)} que anteciparam em {mes_str}.',
    html
)

# fn2-val: Total Vidas Elegíveis
html = re_mod.sub(
    r'(<span class="fn2-val">)[\d,\.]+[kM](</span>\s*<span class="fn2-pct">100%)',
    r'\g<1>' + fk(total_v) + r'\2',
    html
)
# fn2-drop: sem conta PicPay
html = re_mod.sub(
    r'▼ [\d,\.]+[kM] sem conta PicPay',
    f'▼ {fk(sem_conta_funil)} sem conta PicPay',
    html
)
# fn2-val: Com Conta PicPay
html = re_mod.sub(
    r'(<span class="fn2-val">)[\d,\.]+[kM](</span>\s*<span class="fn2-pct">\d+% do total)',
    r'\g<1>' + fk(com_conta) + r'\2',
    html
)
# fn2-drop: com conta mas sem margem
html = re_mod.sub(
    r'▼ [\d,\.]+[kM] com conta mas sem margem habilitada',
    f'▼ {fk(sem_margem_funil)} com conta mas sem margem habilitada',
    html
)
# fn2-val: Conta + Margem Ativa
html = re_mod.sub(
    r'(<span class="fn2-val">)[\d,\.]+[kM](</span>\s*<span class="fn2-pct">\d+% dos c/ conta)',
    r'\g<1>' + fk(com_margem) + r'\2',
    html
)
# fn2-drop: têm margem mas não são MAU
html = re_mod.sub(
    r'▼ [\d,\.]+[kM] têm margem mas não são MAU no app',
    f'▼ {fk(mau_sem_margem_f)} têm margem mas não são MAU no app',
    html
)
# fn2-val: MAU 30d + Margem
html = re_mod.sub(
    r'(<span class="fn2-val">)[\d,\.]+[kM](</span>\s*<span class="fn2-pct">\d+% dos c/ margem)',
    r'\g<1>' + fk(mau_geral_m) + r'\2',
    html
)
# fn2-drop: MAU + margem que não anteciparam
html = re_mod.sub(
    r'▼ [\d,\.]+[kM] MAU \+ margem que',
    f'▼ {fk(nao_anteciparam)} MAU + margem que',
    html
)
# fn2-val: Antecipando (último step — tem style inline)
html = re_mod.sub(
    r'(<span class="fn2-val"[^>]*>)[\d,\.]+[kM](</span>\s*<span class="fn2-pct">\d+% MAU c/ margem)',
    r'\g<1>' + fk(antecipando) + r'\2',
    html
)
# fn2-pct: % MAU c/ margem (último step)
html = re_mod.sub(
    r'(\d+)% MAU c/ margem(</span>)',
    f'{pct_ant_mau}% MAU c/ margem\\2',
    html
)

# Atualizar tag de atualização (timestamp)
ts = datetime.now().strftime("%d/%m/%Y %H:%M")
html = re_mod.sub(r'Dados de \w+/\d+.*?</span>', f'Dados de {mes_str} · Atualizado {ts}</span>', html)
html = re_mod.sub(r'Atualizado em \d{2}/\d{2}/\d{4}', f'Atualizado em {ts}', html)

with open(SCORE_PATH, "w") as f:
    f.write(html)

print(f"[{datetime.now().strftime('%H:%M:%S')}] HTML atualizado: {SCORE_PATH}")

# ── Git push ───────────────────────────────────────────────────────
print(f"[{datetime.now().strftime('%H:%M:%S')}] Publicando no GitHub Pages...")
REPO_DIR = Path.home() / "relatorio-benefits"
import shutil
shutil.copy(SCORE_PATH, REPO_DIR / "score.html")

for cmd in [
    ["git", "-C", str(REPO_DIR), "pull", "--rebase", "--autostash", "origin", "main"],
    ["git", "-C", str(REPO_DIR), "add", "score.html"],
    ["git", "-C", str(REPO_DIR), "commit", "-m", f"data: score.html atualizado {ts}"],
    ["git", "-C", str(REPO_DIR), "push", "origin", "main"],
]:
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0 and "nothing to commit" not in result.stdout:
        print(f"  ⚠️  {' '.join(cmd[-2:])}: {result.stderr[:100]}")

print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Publicado: https://felipetrezza.github.io/relatorio-benefits/score.html")
