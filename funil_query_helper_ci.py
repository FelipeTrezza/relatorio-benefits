"""
funil_query_helper_ci.py — variante para GitHub Actions (CI/CD)
Usa DATABRICKS_HOST + DATABRICKS_TOKEN como env vars (sem OAuth CLI)
Parâmetros via env vars:
  ACTIVITY_NAME  — obrigatório
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

activity   = os.environ.get("ACTIVITY_NAME", "").strip()
date_from  = os.environ.get("DATE_FROM", "").strip() or None
date_to    = os.environ.get("DATE_TO", "").strip() or None
request_id = os.environ.get("REQUEST_ID", "unknown")
host       = os.environ.get("DATABRICKS_HOST", "https://picpay-principal.cloud.databricks.com")
token      = os.environ.get("DATABRICKS_TOKEN", "")

if not activity:
    print(json.dumps({"ok": False, "error": "ACTIVITY_NAME obrigatorio", "request_id": request_id}))
    sys.exit(1)

if not token:
    print(json.dumps({"ok": False, "error": "DATABRICKS_TOKEN nao configurado", "request_id": request_id}))
    sys.exit(1)

# Conectar via token direto (sem CLI OAuth)
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

act_safe = activity.replace("'", "''")

# ── Detectar período se não foi passado ─────────────────────────
if not date_from or not date_to:
    sql_period = (
        "SELECT date_format(min(sent_at),'yyyy-MM-dd') AS dt_from, "
        "       date_format(max(sent_at),'yyyy-MM-dd') AS dt_to, "
        "       count(*) AS total "
        "FROM marketing.consumers_campaigns_communications "
        "WHERE activity_name = '%s'" % act_safe
    )
    try:
        rp = run_q(sql_period, timeout=60)
        if rp["rows"] and rp["rows"][0][0]:
            date_from = date_from or rp["rows"][0][0]
            date_to   = date_to   or rp["rows"][0][1]
        else:
            print(json.dumps({
                "ok": False,
                "error": "Nenhum registro encontrado para: %s" % activity,
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

# ── Query principal ─────────────────────────────────────────────
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
  WHERE activity_name = '%(act)s'
  %(date_filter)s
),
antecip AS (
  SELECT DISTINCT consumer_id, date(created_at) AS dt_antecip
  FROM benefits.anticipation_request
  WHERE request_status = 'FINISH'
),
cruzado AS (
  SELECT DISTINCT c.consumer_id
  FROM comms c
  JOIN antecip a ON c.consumer_id = a.consumer_id
  WHERE date(c.sent_at) <= a.dt_antecip
    AND date(c.sent_at) >= a.dt_antecip - INTERVAL 30 DAYS
)
SELECT
  COALESCE(ms.setor, 'Sem vinculo') AS setor,
  c.channel,
  COUNT(DISTINCT c.consumer_id)                                          AS enviados,
  COUNT(DISTINCT CASE WHEN c.is_delivered = true THEN c.consumer_id END) AS entregues,
  COUNT(DISTINCT CASE WHEN c.is_opened    = true THEN c.consumer_id END) AS abriram,
  COUNT(DISTINCT CASE WHEN c.is_clicked   = true THEN c.consumer_id END) AS clicaram,
  COUNT(DISTINCT cr.consumer_id)                                         AS anteciparam
FROM comms c
LEFT JOIN mapa_setor ms ON c.consumer_id = ms.consumer_id
LEFT JOIN cruzado cr    ON c.consumer_id = cr.consumer_id
GROUP BY COALESCE(ms.setor, 'Sem vinculo'), c.channel
ORDER BY setor, enviados DESC
""" % {"act": act_safe, "date_filter": date_filter}

try:
    r = run_q(sql)
    print(json.dumps({
        "ok":            True,
        "activity_name": activity,
        "date_from":     date_from,
        "date_to":       date_to,
        "request_id":    request_id,
        "rows":          r["rows"],
        "columns":       r["columns"],
    }))
except Exception as e:
    print(json.dumps({
        "ok":         False,
        "error":      str(e),
        "request_id": request_id
    }))
    sys.exit(1)
