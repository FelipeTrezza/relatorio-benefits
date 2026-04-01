"""
Helper: executa query de funil marketing x antecipações no Databricks.
Uso: python3 funil_query_helper.py "activity_name" ["date_from"] ["date_to"]
  date_from / date_to: formato YYYY-MM-DD (opcional — detecta automaticamente se omitido)
"""
import json, time, sys, warnings
warnings.filterwarnings('ignore')

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState
except ImportError:
    print(json.dumps({"ok": False, "error": "databricks-sdk nao instalado. Rode: pip install databricks-sdk"}))
    sys.exit(1)

if len(sys.argv) < 2:
    print(json.dumps({"ok": False, "error": "activity_name obrigatorio como argumento"}))
    sys.exit(1)

activity   = sys.argv[1]
date_from  = sys.argv[2] if len(sys.argv) > 2 else None
date_to    = sys.argv[3] if len(sys.argv) > 3 else None

w  = WorkspaceClient(host="https://picpay-principal.cloud.databricks.com", profile="picpay")
WH = "6077a99f149e0d70"

def run_q(sql, timeout=240):
    resp = w.statement_execution.execute_statement(warehouse_id=WH, statement=sql, wait_timeout="50s")
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
            raise Exception(s.status.error.message)
    raise Exception("timeout")

act_safe = activity.replace("'", "''")

# ── Passo 1: detectar período real se não foi passado ──────────
if not date_from or not date_to:
    sql_period = (
        "SELECT date_format(min(sent_at),'yyyy-MM-dd') AS dt_from, "
        "       date_format(max(sent_at),'yyyy-MM-dd') AS dt_to, "
        "       count(*) AS total "
        "FROM marketing.consumers_campaigns_communications "
        "WHERE activity_name = '__ACT__'"
    ).replace("__ACT__", act_safe)
    try:
        rp = run_q(sql_period, timeout=60)
        if rp["rows"] and rp["rows"][0][0]:
            date_from = date_from or rp["rows"][0][0]
            date_to   = date_to   or rp["rows"][0][1]
        else:
            print(json.dumps({"ok": False, "error": "Nenhum registro encontrado para este activity_name"}))
            sys.exit(0)
    except Exception as e:
        print(json.dumps({"ok": False, "error": "Erro ao detectar periodo: " + str(e)}))
        sys.exit(1)

# ── Passo 2: query principal com filtro de data ────────────────
date_filter = "AND date(sent_at) BETWEEN '__FROM__' AND '__TO__'"\
    .replace("__FROM__", date_from).replace("__TO__", date_to)

sql = (
    "WITH\n"
    "mapa_setor AS (\n"
    "  SELECT co.consumer_id,\n"
    "    CASE\n"
    "      WHEN f.account_id = 1420 OR f.account_id IS NULL THEN 'INSS'\n"
    "      WHEN f.account_id IN (6,52,57,58,60,62,65,73,100,3170,3675) THEN 'Grupo'\n"
    "      WHEN co2.flag_company_sector = 'public' THEN 'Publico'\n"
    "      ELSE 'Privado'\n"
    "    END AS setor\n"
    "  FROM benefits.collaborators co\n"
    "  JOIN benefits.companies co2 ON co.company_id = co2.company_id\n"
    "  LEFT JOIN benefits.accounts f ON co2.account_id = f.account_id\n"
    "),\n"
    "comms AS (\n"
    "  SELECT consumer_id, channel, is_sent, is_delivered, is_opened, is_clicked, sent_at\n"
    "  FROM marketing.consumers_campaigns_communications\n"
    "  WHERE activity_name = '__ACT__'\n"
    "  __DATE_FILTER__\n"
    "),\n"
    "antecip AS (\n"
    "  SELECT DISTINCT consumer_id, date(created_at) AS dt_antecip\n"
    "  FROM benefits.anticipation_request\n"
    "  WHERE request_status = 'FINISH'\n"
    "),\n"
    "cruzado AS (\n"
    "  SELECT DISTINCT c.consumer_id\n"
    "  FROM comms c\n"
    "  JOIN antecip a ON c.consumer_id = a.consumer_id\n"
    "  WHERE date(c.sent_at) <= a.dt_antecip\n"
    "    AND date(c.sent_at) >= a.dt_antecip - INTERVAL 30 DAYS\n"
    ")\n"
    "SELECT\n"
    "  COALESCE(ms.setor, 'Sem vinculo') AS setor,\n"
    "  c.channel,\n"
    "  COUNT(DISTINCT c.consumer_id) AS enviados,\n"
    "  COUNT(DISTINCT CASE WHEN c.is_delivered = true THEN c.consumer_id END) AS entregues,\n"
    "  COUNT(DISTINCT CASE WHEN c.is_opened = true THEN c.consumer_id END) AS abriram,\n"
    "  COUNT(DISTINCT CASE WHEN c.is_clicked = true THEN c.consumer_id END) AS clicaram,\n"
    "  COUNT(DISTINCT cr.consumer_id) AS anteciparam\n"
    "FROM comms c\n"
    "LEFT JOIN mapa_setor ms ON c.consumer_id = ms.consumer_id\n"
    "LEFT JOIN cruzado cr ON c.consumer_id = cr.consumer_id\n"
    "GROUP BY COALESCE(ms.setor, 'Sem vinculo'), c.channel\n"
    "ORDER BY setor, enviados DESC\n"
).replace("__ACT__", act_safe).replace("__DATE_FILTER__", date_filter)

try:
    r = run_q(sql)
    print(json.dumps({
        "ok":            True,
        "activity_name": activity,
        "date_from":     date_from,
        "date_to":       date_to,
        "rows":          r["rows"],
        "columns":       r["columns"],
    }))
except Exception as e:
    print(json.dumps({"ok": False, "error": str(e)}))
