"""
funil_query_helper_ci.py — variante para GitHub Actions (CI/CD)
Usa DATABRICKS_HOST + DATABRICKS_TOKEN como env vars (sem OAuth CLI)
Parâmetros via env vars:
  ACTIVITY_NAME  — buscar por activity_name (um valor específico)
  JOURNEY_NAME   — buscar por journey_name (agrega todas as activities da journey)
  (um dos dois é obrigatório)
  DATE_FROM      — opcional (detecta automaticamente)
  DATE_TO        — opcional (detecta automaticamente)
  REQUEST_ID     — obrigatório (para rastreabilidade)
"""
import json, os, sys, time, warnings
warnings.filterwarnings("ignore")

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState
except ImportError:
    print(json.dumps({"ok": False, "error": "databricks-sdk nao instalado"}))
    sys.exit(1)

activity     = os.environ.get("ACTIVITY_NAME", "").strip()
journey      = os.environ.get("JOURNEY_NAME",  "").strip()
date_from    = os.environ.get("DATE_FROM",     "").strip() or None
date_to      = os.environ.get("DATE_TO",       "").strip() or None
request_id   = os.environ.get("REQUEST_ID",    "unknown")
host         = os.environ.get("DATABRICKS_HOST", "https://picpay-principal.cloud.databricks.com")
token        = os.environ.get("DATABRICKS_TOKEN", "")

# Validações
if not activity and not journey:
    print(json.dumps({"ok": False, "error": "ACTIVITY_NAME ou JOURNEY_NAME obrigatorio", "request_id": request_id}))
    sys.exit(1)
if not token:
    print(json.dumps({"ok": False, "error": "DATABRICKS_TOKEN nao configurado", "request_id": request_id}))
    sys.exit(1)

search_mode = "journey" if journey else "activity"
search_val  = journey if journey else activity

w  = WorkspaceClient(host=host, token=token)
WH = "6077a99f149e0d70"

def run_q(sql, timeout=300):
    resp = w.statement_execution.execute_statement(
        warehouse_id=WH, statement=sql, wait_timeout="50s"
    )
    if resp.status.state == StatementState.SUCCEEDED:
        cols = [c.name for c in resp.manifest.schema.columns]
        rows = resp.result.data_array or []
        return {"columns": cols, "rows": [list(r) for r in rows]}
    sid = resp.statement_id
    for _ in range(timeout // 5):
        time.sleep(5)
        s = w.statement_execution.get_statement(sid)
        if s.status.state == StatementState.SUCCEEDED:
            cols = [c.name for c in s.manifest.schema.columns]
            rows = s.result.data_array or []
            return {"columns": cols, "rows": [list(r) for r in rows]}
        elif s.status.state in (StatementState.FAILED, StatementState.CANCELED):
            raise Exception(s.status.error.message if s.status.error else "query falhou")
    raise Exception("timeout após %ds" % timeout)

val_safe = search_val.replace("'", "''")

# Filtro WHERE dependendo do modo
if search_mode == "journey":
    where_filter = "journey_name = '%s'" % val_safe
    name_label   = journey
else:
    where_filter = "activity_name = '%s'" % val_safe
    name_label   = activity

# ── Se for journey: buscar activities vinculadas + período da journey ───
activities_list = []
journey_date_from = None
journey_date_to   = None

if search_mode == "journey":
    try:
        # Buscar activities E o período completo da journey num único select
        sql_acts = (
            "SELECT "
            "  activity_name, "
            "  COUNT(DISTINCT consumer_id) AS consumers, "
            "  date_format(min(sent_at),'yyyy-MM-dd') AS dt_from, "
            "  date_format(max(sent_at),'yyyy-MM-dd') AS dt_to "
            "FROM marketing.consumers_campaigns_communications "
            "WHERE journey_name = '%s' "
            "GROUP BY activity_name ORDER BY consumers DESC"
        ) % val_safe
        r = run_q(sql_acts, timeout=120)
        activities_list = [row[0] for row in r["rows"]]
        # Período da journey = min(dt_from) de todas as activities → max(dt_to)
        all_from = [row[2] for row in r["rows"] if row[2]]
        all_to   = [row[3] for row in r["rows"] if row[3]]
        if all_from and all_to:
            journey_date_from = min(all_from)
            journey_date_to   = max(all_to)
    except Exception as e:
        activities_list = []

    # ── Fallback: se journey_name não encontrou nada, tentar pelo ID numérico ──
    # Usuários frequentemente digitam nomes com "na" em vez de "id", ou sufixos diferentes
    # Ex: "20260325-na-11560355402-...-mcn-Titulo" → tabela tem "20260325-id-11560355402-...-inappm-4-Titulo"
    import re as _re
    id_match = _re.search(r'[-_](\d{8,})[-_]', val_safe)
    if not activities_list and id_match:
        campaign_id = id_match.group(1)
        try:
            sql_fallback = (
                "SELECT "
                "  activity_name, "
                "  COUNT(DISTINCT consumer_id) AS consumers, "
                "  date_format(min(sent_at),'yyyy-MM-dd') AS dt_from, "
                "  date_format(max(sent_at),'yyyy-MM-dd') AS dt_to "
                "FROM marketing.consumers_campaigns_communications "
                "WHERE activity_name LIKE '%%%s%%' "
                "   OR journey_name  LIKE '%%%s%%' "
                "GROUP BY activity_name ORDER BY consumers DESC LIMIT 50"
            ) % (campaign_id, campaign_id)
            r_fb = run_q(sql_fallback, timeout=120)
            if r_fb["rows"]:
                activities_list = [row[0] for row in r_fb["rows"]]
                all_from_fb = [row[2] for row in r_fb["rows"] if row[2]]
                all_to_fb   = [row[3] for row in r_fb["rows"] if row[3]]
                if all_from_fb: journey_date_from = min(all_from_fb)
                if all_to_fb:   journey_date_to   = max(all_to_fb)
                acts_escaped = ["'" + a.replace("'","''") + "'" for a in activities_list]
                where_filter = "activity_name IN (%s)" % ",".join(acts_escaped)
        except Exception:
            pass

    # ── Enriquecimento: se journey 1:1, buscar pelo ID numérico da campanha ──
    if id_match and len(activities_list) <= 1:
        campaign_id = id_match.group(1).strip('-_')
        try:
            sql_related = (
                "SELECT "
                "  activity_name, "
                "  channel, "
                "  COUNT(DISTINCT consumer_id) AS consumers, "
                "  date_format(min(sent_at),'yyyy-MM-dd') AS dt_from, "
                "  date_format(max(sent_at),'yyyy-MM-dd') AS dt_to "
                "FROM marketing.consumers_campaigns_communications "
                "WHERE activity_name LIKE '%%%s%%' "
                "  AND activity_name != '%s' "
                "GROUP BY activity_name, channel "
                "ORDER BY dt_from, consumers DESC "
                "LIMIT 50"
            ) % (campaign_id, activities_list[0] if activities_list else '')
            r2 = run_q(sql_related, timeout=120)
            related = [row[0] for row in r2["rows"]]
            # Expandir datas com as relacionadas
            all_from2 = [row[3] for row in r2["rows"] if row[3]]
            all_to2   = [row[4] for row in r2["rows"] if row[4]]
            if all_from2:
                journey_date_from = min(filter(None, [journey_date_from] + all_from2))
                journey_date_to   = max(filter(None, [journey_date_to]   + all_to2))
            # Juntar: activities da journey + relacionadas pelo ID (sem duplicatas)
            all_acts = activities_list + [a for a in related if a not in activities_list]
            activities_list = all_acts
            # Atualizar where_filter para cobrir TODAS as activities encontradas
            if activities_list:
                acts_escaped = ["'" + a.replace("'", "''") + "'" for a in activities_list]
                where_filter = "activity_name IN (%s)" % ",".join(acts_escaped)
        except Exception:
            pass  # Não crítico

# ── Detectar período ────────────────────────────────────────────────
# Para journey: usar o range completo da journey (cobre todas as activities)
# Para activity: detectar pelo próprio where_filter
if search_mode == "journey" and journey_date_from and journey_date_to:
    # Período da journey já calculado acima — usar mesmo que date_from/to foram passados
    # pois a intenção é cobrir TODAS as activities
    date_from = journey_date_from
    date_to   = journey_date_to
elif not date_from or not date_to:
    sql_period = (
        "SELECT date_format(min(sent_at),'yyyy-MM-dd') AS dt_from, "
        "       date_format(max(sent_at),'yyyy-MM-dd') AS dt_to, "
        "       count(*) AS total "
        "FROM marketing.consumers_campaigns_communications "
        "WHERE %s"
    ) % where_filter
    try:
        rp = run_q(sql_period, timeout=60)
        if rp["rows"] and rp["rows"][0][0]:
            date_from = date_from or rp["rows"][0][0]
            date_to   = date_to   or rp["rows"][0][1]
        else:
            print(json.dumps({
                "ok": False,
                "error": "Nenhum registro encontrado para: %s" % search_val,
                "request_id": request_id
            }))
            sys.exit(0)
    except Exception as e:
        print(json.dumps({
            "ok": False,
            "error": "Erro ao detectar periodo: %s" % str(e),
            "request_id": request_id
        }))
        sys.exit(1)

# ── Query principal ─────────────────────────────────────────────────
date_filter = "AND date(sent_at) BETWEEN '%s' AND '%s'" % (date_from, date_to)

sql = """
WITH
mapa_setor AS (
  SELECT co.consumer_id,
    CASE
      WHEN f.account_id = 1420 OR f.account_id IS NULL THEN 'INSS'
      WHEN f.account_id IN (6,52,57,58,60,62,65,73,100,3170,3675) THEN 'Grupo'
      WHEN co2.flag_company_sector = 'public' THEN 'Publico'
      ELSE 'Privado'
    END AS setor
  FROM benefits.collaborators co
  JOIN benefits.companies co2 ON co.company_id = co2.company_id
  LEFT JOIN benefits.accounts f ON co2.account_id = f.account_id
),
comms AS (
  SELECT consumer_id, channel, is_sent, is_delivered, is_opened, is_clicked, sent_at
  FROM marketing.consumers_campaigns_communications
  WHERE %(where_filter)s
  %(date_filter)s
),
antecip AS (
  -- Antecipações dos consumers desta campanha dentro da janela de tempo possível
  SELECT anticipation_id, consumer_id, created_at AS ts_antecip
  FROM benefits.anticipation_request
  WHERE request_status = 'FINISH'
    AND consumer_id IN (SELECT DISTINCT consumer_id FROM comms)
),
cruzado AS (
  -- Atribuição: antecipação ocorreu APÓS o envio e dentro de 24h do envio
  -- Uma linha por antecipação (mesmo consumer pode ter múltiplas no período)
  SELECT a.anticipation_id, c.consumer_id
  FROM comms c
  JOIN antecip a ON c.consumer_id = a.consumer_id
  WHERE a.ts_antecip > c.sent_at
    AND a.ts_antecip <= c.sent_at + INTERVAL 24 HOURS
)
SELECT
  COALESCE(ms.setor, 'Sem vinculo') AS setor,
  c.channel,
  COUNT(DISTINCT c.consumer_id)                                          AS enviados,
  COUNT(DISTINCT CASE WHEN c.is_delivered = true THEN c.consumer_id END) AS entregues,
  COUNT(DISTINCT CASE WHEN c.is_opened    = true THEN c.consumer_id END) AS abriram,
  COUNT(DISTINCT CASE WHEN c.is_clicked   = true THEN c.consumer_id END) AS clicaram,
  COUNT(cr.anticipation_id)                                             AS anteciparam
FROM comms c
LEFT JOIN mapa_setor ms ON c.consumer_id = ms.consumer_id
LEFT JOIN cruzado cr    ON c.consumer_id = cr.consumer_id
GROUP BY COALESCE(ms.setor, 'Sem vinculo'), c.channel
ORDER BY setor, enviados DESC
""" % {"where_filter": where_filter, "date_filter": date_filter}

try:
    r = run_q(sql)
    print(json.dumps({
        "ok":             True,
        "search_mode":    search_mode,
        "activity_name":  activity if search_mode == "activity" else None,
        "journey_name":   journey  if search_mode == "journey"  else None,
        "activities":     activities_list,   # lista de activities da journey (se modo journey)
        "display_name":   name_label,        # nome a exibir no funil
        "date_from":      date_from,
        "date_to":        date_to,
        "request_id":     request_id,
        "rows":           r["rows"],
        "columns":        r["columns"],
    }))
except Exception as e:
    print(json.dumps({
        "ok":         False,
        "error":      str(e),
        "request_id": request_id
    }))
    sys.exit(1)
