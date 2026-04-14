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

# ── Query Last Click 7d — lógica correta ─────────────────────────────────────
# Fluxo:
#   1. Para cada antecipação, busca TODAS as comms sfpjbene nos 7d anteriores
#   2. Seleciona a comunicação mais recente (MAX sent_at) → last click real
#   3. O canal dessa comunicação recebe 100% do crédito
#   4. 1ª vida = primeira antecipação da vida caiu nessa conversão
#   5. Recompra = consumer já tinha antecipado antes
# Assim um consumer com múltiplos canais na janela credita apenas o último.
print("[3/4] Buscando conversão last-click (7d, último canal antes da antecipação)...")
_, rows_lc = run_q(f"""
WITH antecipacoes AS (
    -- Todas as antecipações do período histórico
    SELECT
        consumer_id,
        created_at                              AS ts_antecip,
        DATE_FORMAT(created_at, 'yyyy-MM')      AS mes
    FROM benefits.anticipation_request
    WHERE request_status = 'FINISH'
      AND created_at >= '2025-09-01'
      AND created_at < '{hoje}'
),
primeira_antecip_vida AS (
    -- Primeira antecipação de toda a vida de cada consumer
    SELECT consumer_id, MIN(created_at) AS ts_primeira
    FROM benefits.anticipation_request
    WHERE request_status = 'FINISH'
    GROUP BY consumer_id
),
comms_janela AS (
    -- Para cada antecipação, todas as comms sfpjbene nos 7d anteriores
    SELECT
        a.consumer_id,
        a.ts_antecip,
        a.mes,
        c.channel,
        c.sent_at,
        ROW_NUMBER() OVER (
            PARTITION BY a.consumer_id, a.ts_antecip
            ORDER BY c.sent_at DESC          -- mais recente primeiro = last click
        ) AS rn
    FROM antecipacoes a
    JOIN marketing.consumers_campaigns_communications c
        ON CAST(c.consumer_id AS BIGINT) = a.consumer_id
       AND c.sent_at <= a.ts_antecip                     -- comunicação antes da antecipação
       AND c.sent_at >  a.ts_antecip - INTERVAL 7 DAYS  -- dentro dos 7 dias
       AND SPLIT(c.journey_name, '-')[7] = 'sfpjbene'
       AND LOWER(c.journey_name) NOT LIKE '%educacional%'
),
last_click AS (
    -- Apenas o último canal antes de cada antecipação (rn = 1)
    SELECT
        cj.consumer_id,
        cj.ts_antecip,
        cj.mes,
        cj.channel,
        pv.ts_primeira,
        CASE
            WHEN pv.ts_primeira = cj.ts_antecip THEN 'primeira_vida'  -- esta antecipação é a 1ª da vida
            WHEN pv.ts_primeira < cj.ts_antecip THEN 'recompra'       -- já antecipou antes
            ELSE 'nao_classif'
        END AS tipo
    FROM comms_janela cj
    JOIN primeira_antecip_vida pv ON cj.consumer_id = pv.consumer_id
    WHERE cj.rn = 1
)
SELECT
    mes,
    channel,
    tipo,
    COUNT(DISTINCT consumer_id) AS consumers
FROM last_click
WHERE tipo IN ('primeira_vida', 'recompra')
GROUP BY mes, channel, tipo
ORDER BY mes, channel, tipo
""", label="last_click_7d")

# Organizar LC_DADOS: {mes: {primeira_vida: {canal: N}, recompra: {canal: N}}}
lc_dados = {}
for row in rows_lc:
    mes, channel, tipo, consumers = row
    mes      = str(mes)
    channel  = str(channel).upper()
    tipo     = str(tipo)
    consumers = int(consumers or 0)
    if mes not in lc_dados:
        lc_dados[mes] = {"primeira_vida": {}, "recompra": {}}
    lc_dados[mes][tipo][channel] = consumers

print(f"   LC_DADOS: {len(lc_dados)} meses calculados (lógica comunicação → 7d frente)")

# ── Query Recorrência v2 — base last-click (antecipação → 7d para trás) ──────
# Três dimensões com a mesma base de verdade:
#   1. mix_canais  — quantos canais distintos tocaram o consumer nos 7d antes
#   2. last_canal  — qual foi o último canal (já em LC_DADOS, aqui para enriquecer)
#   3. combo_canais— set de canais recebidos (sem ordem), ranking por taxa
# Âncora: antecipação. Janela: 7d para trás. Last-click: sent_at DESC rn=1.
print("[4/5] Buscando recorrência de canais (last-click, 7d para trás)...")
_, rows_rec = run_q(f"""
WITH antecipacoes AS (
    SELECT
        consumer_id,
        created_at                          AS ts_antecip,
        DATE_FORMAT(created_at, 'yyyy-MM')  AS mes
    FROM benefits.anticipation_request
    WHERE request_status = 'FINISH'
      AND created_at >= '2025-09-01'
      AND created_at < '{hoje}'
),
comms_janela AS (
    -- Todas as comms sfpjbene nos 7d antes de cada antecipação
    SELECT
        a.consumer_id,
        a.ts_antecip,
        a.mes,
        c.channel,
        c.sent_at,
        ROW_NUMBER() OVER (
            PARTITION BY a.consumer_id, a.ts_antecip
            ORDER BY c.sent_at DESC   -- mais recente = last click (rn=1)
        ) AS rn
    FROM antecipacoes a
    JOIN marketing.consumers_campaigns_communications c
        ON CAST(c.consumer_id AS BIGINT) = a.consumer_id
       AND c.sent_at <= a.ts_antecip
       AND c.sent_at >  a.ts_antecip - INTERVAL 7 DAYS
       AND SPLIT(c.journey_name, '-')[7] = 'sfpjbene'
       AND LOWER(c.journey_name) NOT LIKE '%educacional%'
),
base AS (
    -- Uma linha por (consumer, antecipação): canais distintos, último canal
    -- Para preservar a ordem last→first no combo, usamos uma CTE ordenada antes do GROUP BY
    SELECT
        mes,
        consumer_id,
        ts_antecip,
        COUNT(DISTINCT channel)            AS n_canais,
        MAX(CASE WHEN rn = 1 THEN channel END) AS last_canal,
        -- Combo ordenado do último para o primeiro canal recebido
        -- collect_list preserva a ordem das linhas; ordenamos por rn ASC (rn=1 = mais recente)
        ARRAY_JOIN(
            ARRAY_DISTINCT(
                collect_list(channel)
            ),
            '+'
        ) AS combo
    FROM (
        SELECT mes, consumer_id, ts_antecip, channel, rn
        FROM comms_janela
        ORDER BY mes, consumer_id, ts_antecip, rn ASC  -- rn=1 = mais recente (sent_at DESC)
    )
    GROUP BY mes, consumer_id, ts_antecip
)
-- 1. Mix de canais: quantos canais distintos o consumer recebeu
SELECT
    'mix'           AS dim,
    mes,
    CAST(n_canais AS STRING)  AS chave,
    COUNT(*)        AS antecipacoes,
    COUNT(DISTINCT consumer_id) AS consumers
FROM base
GROUP BY mes, n_canais

UNION ALL

-- 2. Last canal: qual foi o último canal antes da antecipação
SELECT
    'last'          AS dim,
    mes,
    last_canal      AS chave,
    COUNT(*)        AS antecipacoes,
    COUNT(DISTINCT consumer_id) AS consumers
FROM base
WHERE last_canal IS NOT NULL
GROUP BY mes, last_canal

UNION ALL

-- 3. Combo de canais: set de canais recebidos nos 7d (sem ordem)
SELECT
    'combo'         AS dim,
    mes,
    combo           AS chave,
    COUNT(*)        AS antecipacoes,
    COUNT(DISTINCT consumer_id) AS consumers
FROM base
WHERE combo IS NOT NULL AND combo != ''
GROUP BY mes, combo

ORDER BY dim, mes, antecipacoes DESC
""", label="recorrencia_lc")

# Organizar REC2_DADOS: {mes: {mix: [{n_canais, antecipacoes, consumers}], last: {canal: {ant, cons}}, combo: [...]}}
rec2 = {}
for row in rows_rec:
    dim, mes, chave, antecipacoes, consumers = row
    mes = str(mes); dim = str(dim); chave = str(chave)
    antecipacoes = int(antecipacoes or 0); consumers = int(consumers or 0)
    if mes not in rec2:
        rec2[mes] = {"mix": [], "last": {}, "combo": []}
    if dim == "mix":
        rec2[mes]["mix"].append({"n": int(chave), "ant": antecipacoes, "cons": consumers})
    elif dim == "last":
        rec2[mes]["last"][chave] = {"ant": antecipacoes, "cons": consumers}
    elif dim == "combo":
        rec2[mes]["combo"].append({"combo": chave, "ant": antecipacoes, "cons": consumers})

# Ordenar mix por n_canais, combo por antecipacoes desc (já vem da query)
for mes in rec2:
    rec2[mes]["mix"].sort(key=lambda x: x["n"])
    rec2[mes]["combo"] = rec2[mes]["combo"][:30]  # top 30 combos

print(f"   REC2_DADOS: {len(rec2)} meses | ex Mar/26 mix={len(rec2.get('2026-03',{}).get('mix',[]))} grupos")

print("[5/5] Calculando métricas e atualizando HTML...")

# ── Custo por canal ───────────────────────────────────────────────────────────
CUSTO = {
    "EMAIL":    {"unit": 0.00044, "base": "enviados"},
    "PUSH":     {"unit": 0.00044, "base": "enviados"},
    "INAPP":    {"unit": 0.00044, "base": "enviados"},
    "DM":       {"unit": 0.006,   "base": "enviados"},
    "WHATSAPP": {"unit": 0.3092,  "base": "enviados"},  # custo por comunicação enviada
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

def fmt_compacto(v):
    """Formato compacto: 1,18M / 895k / 245"""
    if v >= 1_000_000: return f"{v/1_000_000:.2f}M".replace(".",",")
    if v >= 1_000:     return f"{v/1_000:.0f}k"
    return str(v)

html = re.sub(r'(<div class="hk-val"[^>]*>)[^<]+(</div><div class="hk-lbl">Comunicações)',
              f'\\g<1>{fmt_compacto(total_env)}\\2', html)
html = re.sub(r'(<div class="hk-val"[^>]*>)[^<]+(</div><div class="hk-lbl">CAC Médio Ponderado)',
              f'\\g<1>R${cac_medio:.2f}\\2'.replace(".",","), html)
html = re.sub(r'(<div class="hk-val"[^>]*>)[^<]+(</div><div class="hk-lbl">Custo Total MKT)',
              f'\\g<1>{fmt_brl(total_custo)}\\2', html)

# ─ KPI cards visão geral ─────────────────────────────────────────────────────
conv_pct_str = f"{total_conv/total_env*100:.2f}%".replace(".",",")
cac_val_str  = f"R${cac_medio:.2f}".replace(".",",")
custo_str    = fmt_brl(total_custo)

html = re.sub(r'(<div class="kpi-val"[^>]*>)[^<]+(</div>\s*<div class="kpi-sub">5 canais)',
              f'\\g<1>{fmt_num(total_env)}\\2', html)
# kpi-total-env por ID
html = re.sub(r'(<div class="kpi-val"[^>]*id="kpi-total-env"[^>]*>)[^<]+(</div>)',
              f'\\g<1>{fmt_num(total_env)}\\2', html)
# kpi-total-conv por ID (mais robusto que pelo sub-text)
html = re.sub(r'(<div class="kpi-val"[^>]*id="kpi-total-conv"[^>]*>)[^<]+(</div>)',
              f'\\g<1>{fmt_num(total_conv)}\\2', html)
# kpi-conv-pct por ID
html = re.sub(r'(<div class="kpi-val"[^>]*id="kpi-conv-pct"[^>]*>)[^<]+(</div>)',
              f'\\g<1>{conv_pct_str}\\2', html)
# kpi-sub "Convertidos / Enviados" (fallback genérico)
html = re.sub(r'(<div class="kpi-val"[^>]*>)[^<]+(</div>\s*<div class="kpi-sub">Convertidos / Enviados)',
              f'\\g<1>{conv_pct_str}\\2', html)
# kpi-custo-total por ID + kpi-sub "Custo total X" (atualiza os dois juntos)
html = re.sub(
    r'(<div class="kpi-val"[^>]*id="kpi-custo-total"[^>]*>)[^<]+(</div>\s*<div class="kpi-sub">)Custo total[^<]*(</div>)',
    f'\\g<1>{custo_str}\\2Custo total {custo_str}\\3', html
)
# kpi-cac-medio por ID + kpi-sub "R$X / N.NNN conv." (atualiza os dois juntos)
html = re.sub(
    r'(<div class="kpi-val"[^>]*id="kpi-cac-medio"[^>]*>)[^<]+(</div>\s*<div class="kpi-sub">)R\$[\d\.\,]+ / [\d\.]+ conv\.(</div>)',
    f'\\g<1>{cac_val_str}\\2{custo_str} / {fmt_num(total_conv)} conv.\\3',
    html
)

# ─ Hero KPIs: Convertidos e Conv. Geral (hk-val não têm IDs) ─────────────────
html = re.sub(r'(<div class="hk-val"[^>]*>)[^<]+(</div><div class="hk-lbl">Convertidos)',
              f'\\g<1>{fmt_num(total_conv)}\\2', html)
html = re.sub(r'(<div class="hk-val"[^>]*>)[^<]+(</div><div class="hk-lbl">Conv\. Geral)',
              f'\\g<1>{conv_pct_str}\\2', html)

# ─ Linha TOTAL da tabela de canais ───────────────────────────────────────────
# Col order: TOTAL | — | conv | conv% | custo | CAC
# [a] conv
html = re.sub(
    r'(<td colspan="2"><strong>TOTAL</strong></td>\s*<td class="r">—</td>\s*<td class="r">)[\d\.]+',
    f'\\g<1>{fmt_num(total_conv)}', html
)
# [b] conv% — busca a célula após conv
html = re.sub(
    r'(TOTAL</strong></td>[^<]*<td[^>]*>—</td>[^<]*<td[^>]*>[\d\.]+</td>[^<]*<td[^>]*>)[\d,]+%',
    f'\\g<1>{conv_pct_str}', html
)
# [c] custo
html = re.sub(
    r'(TOTAL</strong></td>[^<]*<td[^>]*>—</td>[^<]*<td[^>]*>[\d\.]+</td>[^<]*<td[^>]*>[\d,]+%</td>[^<]*<td[^>]*>)R\$[\d\.\,]+k?',
    f'\\g<1>{custo_str}', html
)
# [d] CAC — célula com color orange
html = re.sub(
    r'(<td class="r" style="color:var\(--orange\)">)R\$[\d,\.]+(</td>\s*<td></td>)',
    f'\\g<1>{cac_val_str}\\2', html
)

# ─ Rodapé data ───────────────────────────────────────────────────────────────
html = re.sub(
    r'(Referência )[A-Za-záéíóúÁÉÍÓÚ]+/\d{4}',
    f'\\g<1>{MES_LABEL}', html
)

# ─ LC_DADOS — substituir dados last-click com nova lógica (comunicação → 7d) ─
if lc_dados:
    # Mesclar com o que já existe no HTML para preservar meses históricos
    lc_match = re.search(r'const LC_DADOS = ({.*?});', html, re.DOTALL)
    if lc_match:
        try:
            lc_existente = json.loads(lc_match.group(1))
        except Exception:
            lc_existente = {}
    else:
        lc_existente = {}

    # Atualizar com os dados recém calculados (sobrescreve meses existentes)
    lc_existente.update(lc_dados)

    lc_json = json.dumps(lc_existente, ensure_ascii=False, separators=(',', ':'))
    html, n = re.subn(
        r'const LC_DADOS = ({.*?});',
        f'const LC_DADOS = {lc_json};',
        html, count=1, flags=re.DOTALL
    )
    if n:
        print(f"  ✅ LC_DADOS atualizado — {len(lc_existente)} meses")
    else:
        print("  ⚠️  LC_DADOS: padrão não encontrado no HTML")
else:
    print("  ⚠️  LC_DADOS: nenhuma linha retornada pela query")

# ─ REC2_DADOS — substituir dados de recorrência com lógica last-click ────────
if rec2:
    rec2_json = json.dumps(rec2, ensure_ascii=False, separators=(',', ':'))
    html, n = re.subn(
        r'const REC2_DADOS = ({.*?});',
        f'const REC2_DADOS = {rec2_json};',
        html, count=1, flags=re.DOTALL
    )
    if n:
        print(f"  ✅ REC2_DADOS atualizado — {len(rec2)} meses")
    else:
        print("  ⚠️  REC2_DADOS: padrão não encontrado no HTML (primeira carga?)")
else:
    print("  ⚠️  REC2_DADOS: nenhuma linha retornada pela query")

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
