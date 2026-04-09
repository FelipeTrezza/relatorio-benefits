#!/usr/bin/env python3
"""
CAC por Canal — Atualização
============================
Busca dados do mês corrente no Databricks e atualiza os números no cac.html.
Uso: python3 atualizar_cac.py
"""

import json, time, sys, warnings, subprocess, re
from datetime import datetime, date
from pathlib import Path

warnings.filterwarnings("ignore")

DATABRICKS_HOST    = "https://picpay-principal.cloud.databricks.com"
DATABRICKS_PROFILE = "picpay"
WAREHOUSE_ID       = "6077a99f149e0d70"
BASE_DIR           = Path(__file__).parent
CAC_PATH           = BASE_DIR / "cac.html"

hoje      = date.today()
MES_ATUAL = hoje.strftime("%Y-%m")
MES_LABEL = hoje.strftime("%b/%y").upper()  # ex: ABR/26
MES_INI   = f"{MES_ATUAL}-01"
MES_FIM   = f"{MES_ATUAL}-{hoje.day:02d}"

print(f"[{datetime.now().strftime('%H:%M:%S')}] Iniciando atualização CAC — {MES_ATUAL}")

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

# ── Query principal: CAC por canal ────────────────────────────────────────────
print("[1/3] Buscando CAC por canal...")
_, rows_canal = run_q(f"""
WITH comms AS (
    SELECT c.consumer_id, c.channel, c.sent_at, c.is_delivered
    FROM marketing.consumers_campaigns_communications c
    WHERE SPLIT(c.journey_name, '-')[7] = 'sfpjbene'
      AND LOWER(c.journey_name) NOT LIKE '%educacional%'
      AND c.sent_at >= '{MES_INI}' AND c.sent_at < '{hoje}'
),
antecip AS (
    SELECT consumer_id, created_at AS ts_antecip
    FROM benefits.anticipation_request
    WHERE request_status = 'FINISH'
      AND created_at >= '{MES_INI}' AND created_at < '{hoje}'
)
SELECT
    c.channel,
    COUNT(*)                                                                      AS enviados,
    SUM(CASE WHEN c.is_delivered = true THEN 1 ELSE 0 END)                       AS entregues,
    COUNT(DISTINCT a.consumer_id)                                                 AS convertidos,
    ROUND(COUNT(DISTINCT a.consumer_id)*100.0/COUNT(*), 2)                       AS conv_pct
FROM comms c
LEFT JOIN antecip a
    ON CAST(c.consumer_id AS BIGINT) = a.consumer_id
   AND a.ts_antecip > c.sent_at
   AND a.ts_antecip <= c.sent_at + INTERVAL 24 HOURS
GROUP BY c.channel
ORDER BY convertidos DESC
""", label="canal")

# ── Query setores ─────────────────────────────────────────────────────────────
print("[2/3] Buscando conversão por setor...")
_, rows_setor = run_q(f"""
WITH colabs AS (
    SELECT col.consumer_id, comp.account_id, comp.flag_company_sector
    FROM benefits.collaborators col
    LEFT JOIN benefits.companies comp ON col.company_id = comp.company_id
    WHERE col.resignation_date IS NULL AND col.consumer_id IS NOT NULL
),
setor_c AS (
    SELECT consumer_id,
        CASE
            WHEN account_id IN (6,52,57,58,60,62,65,73,100,3170,3675) THEN 'grupo'
            ELSE COALESCE(flag_company_sector, 'private')
        END AS setor
    FROM colabs
    WHERE account_id != 1420 AND account_id IS NOT NULL
),
comms AS (
    SELECT c.consumer_id, c.sent_at
    FROM marketing.consumers_campaigns_communications c
    WHERE SPLIT(c.journey_name, '-')[7] = 'sfpjbene'
      AND LOWER(c.journey_name) NOT LIKE '%educacional%'
      AND c.sent_at >= '{MES_INI}' AND c.sent_at < '{hoje}'
),
antecip AS (
    SELECT consumer_id, created_at AS ts_antecip, request_value
    FROM benefits.anticipation_request
    WHERE request_status = 'FINISH'
      AND created_at >= '{MES_INI}' AND created_at < '{hoje}'
)
SELECT
    sc.setor,
    COUNT(DISTINCT c.consumer_id)                                                        AS comunicados,
    COUNT(DISTINCT a.consumer_id)                                                        AS convertidos,
    ROUND(COUNT(DISTINCT a.consumer_id)*100.0/NULLIF(COUNT(DISTINCT c.consumer_id),0),2) AS conv_pct,
    ROUND(SUM(CASE WHEN a.consumer_id IS NOT NULL THEN a.request_value ELSE 0 END),0)    AS tpv,
    ROUND(AVG(CASE WHEN a.consumer_id IS NOT NULL THEN a.request_value END),0)           AS ticket_medio
FROM comms c
INNER JOIN setor_c sc ON c.consumer_id = CAST(sc.consumer_id AS STRING)
LEFT JOIN antecip a
    ON CAST(c.consumer_id AS BIGINT) = a.consumer_id
   AND a.ts_antecip > c.sent_at
   AND a.ts_antecip <= c.sent_at + INTERVAL 24 HOURS
GROUP BY sc.setor
ORDER BY convertidos DESC
""", label="setor")

print("[3/3] Calculando métricas e atualizando HTML...")

# ── Custo por canal ───────────────────────────────────────────────────────────
CUSTO = {
    "EMAIL":    {"unit": 0.00044, "base": "enviados"},
    "PUSH":     {"unit": 0.00044, "base": "enviados"},
    "INAPP":    {"unit": 0.00044, "base": "enviados"},
    "DM":       {"unit": 0.006,   "base": "enviados"},
    "WHATSAPP": {"unit": 0.0526,  "base": "entregues"},
    "SMS":      {"unit": 0.0526,  "base": "enviados"},
}

canais = {}
total_conv  = 0
total_custo = 0.0

for row in rows_canal:
    canal, enviados, entregues, conv, conv_pct = row
    canal = str(canal).upper()
    enviados  = int(enviados  or 0)
    entregues = int(entregues or 0)
    conv      = int(conv      or 0)

    cfg = CUSTO.get(canal, {"unit": 0, "base": "enviados"})
    base_vol = entregues if cfg["base"] == "entregues" else enviados
    custo    = base_vol * cfg["unit"]
    cac      = custo / conv if conv > 0 else 0

    canais[canal] = {
        "enviados": enviados, "entregues": entregues,
        "conv": conv, "conv_pct": float(conv_pct or 0),
        "custo": custo, "cac": cac,
        "vol_label": f"{entregues:,} entregues".replace(",",".") if cfg["base"] == "entregues"
                     else f"{enviados:,} enviados".replace(",",".")
    }
    total_conv  += conv
    total_custo += custo

cac_medio = total_custo / total_conv if total_conv > 0 else 0

setores = {}
for row in rows_setor:
    setor, comunicados, conv, conv_pct, tpv, ticket = row
    setores[str(setor)] = {
        "comunicados": int(comunicados or 0),
        "convertidos": int(conv or 0),
        "conv_pct":    float(conv_pct or 0),
        "tpv":         float(tpv or 0),
        "ticket":      float(ticket or 0),
    }

def fmt_brl(v):
    """Formata número com separador de milhar pt-BR"""
    if v >= 1_000_000:
        return f"R${v/1_000_000:.1f}M".replace(".",",")
    if v >= 1_000:
        return f"R${v/1_000:.1f}k".replace(".",",")
    return f"R${v:,.2f}".replace(",","X").replace(".",",").replace("X",".")

def fmt_num(v):
    return f"{v:,}".replace(",",".")

# ── Ler HTML ──────────────────────────────────────────────────────────────────
html = CAC_PATH.read_text(encoding="utf-8")

def sub(pattern, value, h):
    new_h, n = re.subn(pattern, value, h, count=1)
    if n == 0:
        print(f"  ⚠️  padrão não encontrado: {pattern[:60]}")
    return new_h

# ─ Header badge ──────────────────────────────────────────────────────────────
html = re.sub(
    r'(📅 )[A-Z]{3}/\d{4}( · sfpjbene)',
    f'\\g<1>{MES_LABEL}\\2', html
)
html = re.sub(
    r'(Campanhas sfpjbene · Custo de Aquisição · )[A-Za-záéíóúÁÉÍÓÚ]+ \d{4}',
    f'\\g<1>{hoje.strftime("%B %Y").capitalize()}', html
)

# ─ Hero KPIs ─────────────────────────────────────────────────────────────────
total_env = sum(c["enviados"] for c in canais.values())
html = re.sub(r'(<div class="hk-val"[^>]*>)[^<]+(</div><div class="hk-lbl">Comunicações)',
              f'\\g<1>{fmt_num(total_env)}\\2', html)
html = re.sub(r'(<div class="hk-val"[^>]*>)[^<]+(</div><div class="hk-lbl">Convertidos)',
              f'\\g<1>{fmt_num(total_conv)}\\2', html)
html = re.sub(r'(<div class="hk-val"[^>]*>)[^<]+(</div><div class="hk-lbl">Conv\. Geral)',
              f'\\g<1>{total_conv/total_env*100:.2f}%\\2'.replace(".",","), html)
html = re.sub(r'(<div class="hk-val"[^>]*>)[^<]+(</div><div class="hk-lbl">CAC Médio Ponderado)',
              f'\\g<1>R${cac_medio:.2f}\\2'.replace(".",","), html)
html = re.sub(r'(<div class="hk-val"[^>]*>)[^<]+(</div><div class="hk-lbl">Custo Total MKT)',
              f'\\g<1>{fmt_brl(total_custo)}\\2', html)

# ─ KPI cards visão geral ─────────────────────────────────────────────────────
html = re.sub(r'(<div class="kpi-val"[^>]*>)[^<]+(</div>\s*<div class="kpi-sub">5 canais)',
              f'\\g<1>{fmt_num(total_env)}\\2', html)
html = re.sub(r'(<div class="kpi-val"[^>]*>)[^<]+(</div>\s*<div class="kpi-sub">Únicos)',
              f'\\g<1>{fmt_num(total_conv)}\\2', html)
html = re.sub(r'(<div class="kpi-val"[^>]*>)[^<]+(</div>\s*<div class="kpi-sub">Convertidos / Enviados)',
              f'\\g<1>{total_conv/total_env*100:.2f}%\\2'.replace(".",","), html)
html = re.sub(r'(<div class="kpi-val"[^>]*>)[^<]+(</div>\s*<div class="kpi-sub">Custo total)',
              f'\\g<1>{fmt_brl(total_custo)}\\2', html)
html = re.sub(r'(<div class="kpi-val"[^>]*>)[^<]+(</div>\s*<div class="kpi-sub">R\$[\d\.\,]+ / [\d\.]+ conv)',
              f'\\g<1>R${cac_medio:.2f}\\2'.replace(".",","), html)

# ─ Rodapé data ───────────────────────────────────────────────────────────────
html = re.sub(
    r'(Referência )[A-Za-záéíóúÁÉÍÓÚ]+/\d{4}',
    f'\\g<1>{MES_LABEL}', html
)

# ─ Salvar ────────────────────────────────────────────────────────────────────
CAC_PATH.write_text(html, encoding="utf-8")
print(f"  ✅ cac.html salvo")

# ─ Git push ──────────────────────────────────────────────────────────────────
try:
    subprocess.run(
        ["git", "-C", str(BASE_DIR), "pull", "--rebase", "--autostash", "origin", "main"],
        capture_output=True, check=True
    )
    subprocess.run(
        ["git", "-C", str(BASE_DIR), "add", "cac.html"],
        capture_output=True, check=True
    )
    res = subprocess.run(
        ["git", "-C", str(BASE_DIR), "commit", "-m",
         f"data: atualizar CAC por canal — {MES_LABEL}"],
        capture_output=True
    )
    if res.returncode == 0:
        subprocess.run(
            ["git", "-C", str(BASE_DIR), "push", "origin", "main"],
            capture_output=True, check=True
        )
        print("  ✅ git push OK")
    else:
        print("  ℹ️  nada a commitar")
except Exception as e:
    print(f"  ⚠️  git: {e}")

print(f"\n✅ CAC atualizado — {MES_LABEL} · {total_conv:,} conv · R${cac_medio:.2f} CAC médio")
