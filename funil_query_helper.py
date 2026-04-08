"""
Helper: executa query de funil marketing x antecipações no Databricks.
Uso:
  python3 funil_query_helper.py "activity_name" [date_from] [date_to]
  python3 funil_query_helper.py "journey_name"  [date_from] [date_to] --journey
  date_from / date_to: formato YYYY-MM-DD (opcional)
"""
import json, time, sys, warnings
warnings.filterwarnings('ignore')

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState
except ImportError:
    print(json.dumps({"ok": False, "error": "databricks-sdk nao instalado"}))
    sys.exit(1)

if len(sys.argv) < 2:
    print(json.dumps({"ok": False, "error": "activity/journey obrigatorio como argumento"}))
    sys.exit(1)

args       = sys.argv[1:]
is_journey = '--journey' in args
args       = [a for a in args if a != '--journey']

search_val = args[0]
date_from  = args[1] if len(args) > 1 else None
date_to    = args[2] if len(args) > 2 else None

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

val_safe = search_val.replace("'", "''")

# ── Modo journey: detectar activities + período ─────────────────
activities_list   = []
journey_date_from = None
journey_date_to   = None

if is_journey:
    sql_acts = (
        "SELECT activity_name, COUNT(DISTINCT consumer_id) AS consumers, "
        "date_format(min(sent_at),'yyyy-MM-dd') AS dt_from, "
        "date_format(max(sent_at),'yyyy-MM-dd') AS dt_to "
        "FROM marketing.consumers_campaigns_communications "
        "WHERE journey_name = '%s' "
        "GROUP BY activity_name ORDER BY consumers DESC"
    ) % val_safe
    try:
        r = run_q(sql_acts, timeout=120)
        activities_list = [row[0] for row in r["rows"]]
        all_from = [row[2] for row in r["rows"] if row[2]]
        all_to   = [row[3] for row in r["rows"] if row[3]]
        if all_from and all_to:
            journey_date_from = min(all_from)
            journey_date_to   = max(all_to)
    except Exception as e:
        activities_list = []

    # ── Fallback: se journey_name exato não encontrou nada, buscar pelo ID numérico ──
    # Usuários frequentemente digitam nomes com "na" em vez de "id" ou sufixos diferentes
    # Ex: "20260325-na-11560355402-...-mcn-Titulo" → tabela tem "20260325-id-11560355402-...-inappm-4-Titulo"
    import re as _re
    id_match = _re.search(r'[-_](\d{8,})[-_]', val_safe)
    if not activities_list and id_match:
        campaign_id = id_match.group(1)
        try:
            sql_fallback = (
                "SELECT activity_name, COUNT(DISTINCT consumer_id) AS consumers, "
                "date_format(min(sent_at),'yyyy-MM-dd') AS dt_from, "
                "date_format(max(sent_at),'yyyy-MM-dd') AS dt_to "
                "FROM marketing.consumers_campaigns_communications "
                "WHERE activity_name LIKE '%%%s%%' OR journey_name LIKE '%%%s%%' "
                "GROUP BY activity_name ORDER BY consumers DESC LIMIT 50"
            ) % (campaign_id, campaign_id)
            r_fb = run_q(sql_fallback, timeout=120)
            if r_fb["rows"]:
                activities_list = [row[0] for row in r_fb["rows"]]
                all_from_fb = [row[2] for row in r_fb["rows"] if row[2]]
                all_to_fb   = [row[3] for row in r_fb["rows"] if row[3]]
                journey_date_from = min(all_from_fb) if all_from_fb else journey_date_from
                journey_date_to   = max(all_to_fb)   if all_to_fb   else journey_date_to
        except Exception:
            pass

    if not activities_list:
        print(json.dumps({"ok": False, "error": "Nenhuma activity encontrada. ID buscado: %s" % (id_match.group(1) if id_match else val_safe)}))
        sys.exit(0)

    # Usar período da journey se não foi passado
    date_from = date_from or journey_date_from
    date_to   = date_to   or journey_date_to
    # where_comms: se fallback por ID, cobrir todas as activities encontradas; senão, filtrar por journey_name
    if id_match and not any(True for _ in [1]):  # placeholder — ver abaixo
        where_comms = "journey_name = '%s'" % val_safe
    acts_escaped = ["'" + a.replace("'","''") + "'" for a in activities_list]
    where_comms = "activity_name IN (%s)" % ",".join(acts_escaped)
else:
    where_comms = "activity_name = '%s'" % val_safe

# ── Detectar período se ainda não tem ──────────────────────────
if not date_from or not date_to:
    sql_period = (
        "SELECT date_format(min(sent_at),'yyyy-MM-dd'), "
        "       date_format(max(sent_at),'yyyy-MM-dd') "
        "FROM marketing.consumers_campaigns_communications "
        "WHERE %s"
    ) % where_comms
    try:
        rp = run_q(sql_period, timeout=60)
        if rp["rows"] and rp["rows"][0][0]:
            date_from = date_from or rp["rows"][0][0]
            date_to   = date_to   or rp["rows"][0][1]
        else:
            print(json.dumps({"ok": False, "error": "Nenhum registro encontrado"}))
            sys.exit(0)
    except Exception as e:
        print(json.dumps({"ok": False, "error": "Erro ao detectar periodo: " + str(e)}))
        sys.exit(1)

# ── Query principal otimizada ───────────────────────────────────
sql = """
WITH
comms AS (
  SELECT consumer_id, channel, is_sent, is_delivered, is_opened, is_clicked, sent_at
  FROM marketing.consumers_campaigns_communications
  WHERE {where_comms}
  AND date(sent_at) BETWEEN '{date_from}' AND '{date_to}'
),
mapa_setor AS (
  -- Filtrado nos consumer_ids de comms — evita full scan de 1.9M rows
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
  WHERE co.consumer_id IN (SELECT DISTINCT consumer_id FROM comms)
),
antecip AS (
  -- Antecipações dos consumers desta campanha dentro da janela de tempo possível
  SELECT anticipation_id, consumer_id, created_at AS ts_antecip
  FROM benefits.anticipation_request
  WHERE request_status = 'FINISH'
    AND consumer_id IN (SELECT DISTINCT consumer_id FROM comms)
    AND created_at >= '{date_from}'
    AND created_at <= date_add(cast('{date_to}' AS date), 1)
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
  COUNT(DISTINCT c.consumer_id)                                              AS enviados,
  COUNT(DISTINCT CASE WHEN c.is_delivered = true THEN c.consumer_id END)    AS entregues,
  COUNT(DISTINCT CASE WHEN c.is_opened    = true THEN c.consumer_id END)    AS abriram,
  COUNT(DISTINCT CASE WHEN c.is_clicked   = true THEN c.consumer_id END)    AS clicaram,
  COUNT(cr.anticipation_id)                                                 AS anteciparam
FROM comms c
LEFT JOIN mapa_setor ms ON c.consumer_id = ms.consumer_id
LEFT JOIN cruzado cr    ON c.consumer_id = cr.consumer_id
GROUP BY COALESCE(ms.setor, 'Sem vinculo'), c.channel
ORDER BY setor, enviados DESC
""".format(where_comms=where_comms, date_from=date_from, date_to=date_to)

try:
    r = run_q(sql)
    result = {
        "ok":            True,
        "search_mode":   "journey" if is_journey else "activity",
        "activity_name": search_val if not is_journey else "",
        "journey_name":  search_val if is_journey else "",
        "display_name":  search_val,
        "date_from":     date_from,
        "date_to":       date_to,
        "rows":          r["rows"],
        "columns":       r["columns"],
    }
    if is_journey and activities_list:
        result["activities"] = activities_list
    print(json.dumps(result))
except Exception as e:
    print(json.dumps({"ok": False, "error": str(e)}))
