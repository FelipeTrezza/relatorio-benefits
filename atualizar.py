#!/usr/bin/env python3
"""
Relatório Abertura de Contas — Script de Atualização Diária
============================================================
Uso: python3 atualizar.py

O script:
  1. Conecta no Databricks via OAuth (abre browser na 1ª vez)
  2. Roda a query de abertura de contas
  3. Gera o HTML com dados frescos
  4. Publica via git push → GitHub Pages (link permanente)

Link fixo do relatório (sempre atualizado):
  https://felipetrezza.github.io/relatorio-benefits/
"""

import json
import time
import sys
import os
import warnings
import subprocess
from datetime import datetime
from pathlib import Path

warnings.filterwarnings("ignore")

# ─── CONFIGURAÇÃO ─────────────────────────────────────────────────────────────
DATABRICKS_HOST  = "https://picpay-principal.cloud.databricks.com"
DATABRICKS_PROFILE = "picpay"
WAREHOUSE_ID     = "6077a99f149e0d70"   # Exploracao 04 (trocar se necessário)
SCRIPT_DIR       = Path(__file__).parent
TEMPLATE_PATH    = SCRIPT_DIR / "template.html"
OUTPUT_PATH      = SCRIPT_DIR / "relatorio-abertura-contas.html"
GITHUB_PAGES_URL = "https://felipetrezza.github.io/relatorio-benefits/"

SQL = """
with base_docs as (
  select collaborator_document, company_id, collaborator_id from benefits.sec_collaborators
  qualify row_number() over (partition by collaborator_document order by collaborator_id desc) = 1
), contas as (
  select consumer_id, cpf from consumers.sec_consumers
  where cpf in (select collaborator_document from base_docs)
), abertura as (
  select consumer_id, date_format(sent_bacen_at, 'yyyy-MM-dd') as sent_bacen_at
  from consumers.consumers
  where consumer_id in (select consumer_id from contas)
    and sent_bacen_at is not null
  qualify row_number() over (partition by consumer_id order by sent_bacen_at desc) = 1
), pri_antecipa as (
  select consumer_id, min(date_format(created_at, 'yyyy-MM-dd')) as pri_antecipa
  from benefits.anticipation_request
  where request_status = 'FINISH'
    and consumer_id in (select distinct consumer_id from abertura)
  group by all
), fim as (
  select
    a.collaborator_document,
    a.collaborator_id,
    c.consumer_id,
    a.company_id,
    upper(e.company_name)   as company_name,
    upper(f.account_name)   as account_name,
    f.account_id,
    case
      when f.account_id = 1420                                              then 'INSS'
      when f.account_id in (6,52,57,58,60,62,65,73,100,3170,3675)          then 'grupo'
      when f.account_id is null                                             then 'INSS'
      else coalesce(e.flag_company_sector, 'private')
    end as setor,
    e.address_state,
    upper(e.address_city) as address_city,
    CASE
      when e.address_state in ('AC','AP','AM','PA','RO','RR','TO')          then 'Norte'
      when e.address_state in ('AL','BA','CE','MA','PB','PE','PI','RN','SE') then 'Nordeste'
      when e.address_state in ('DF','GO','MT','MS')                         then 'Centro-Oeste'
      when e.address_state in ('ES','MG','RJ','SP')                         then 'Sudeste'
      when e.address_state in ('PR','RS','SC')                              then 'Sul'
      else 'Outra/Não Identificada'
    end as regiao_brasil,
    CASE
      when upper(account_name) = 'MUNICIPIO DE VOLTA REDONDA'              then 'VOLTA REDONDA'
      when upper(account_name) = 'MARANHÃO PÚBLICO'                        then 'MARANHAO'
      when upper(account_name) = 'ESTADO DO RIO DE JANEIRO'                then 'RIO DE JANEIRO'
      when upper(account_name) = 'GOVERNO DO ESTADO DO PIAUÍ'              then 'PIAUI'
      when upper(account_name) = 'MUNICÍPIO DUQUE DE CAXIAS'               then 'DUQUE DE CAXIAS'
      when upper(account_name) = 'MUNICIPIO DE PETROLINA'                  then 'PETROLINA'
      when upper(account_name) = 'ESTADO DE SANTA CATARINA'                then 'SANTA CATARINA'
      when upper(account_name) = 'MUNICIPIO DE BOA VISTA'                  then 'BOA VISTA'
      when upper(account_name) = 'MUNICIPIO DE CAMBORIU'                   then 'CAMBORIU'
      when upper(account_name) = 'MUNICIPIO DE BALNEARIO CAMBORIU'         then 'BALNEARIO CAMBORIU'
      when upper(account_name) = 'MUNICIPIO DE CHAPECÓ'                    then 'CHAPECÓ'
      when upper(account_name) = 'MUNICIPIO DE JAGUARIUNA'                 then 'JAGUARIUNA'
      when upper(account_name) = 'MUNICIPIO DE PATOS'                      then 'PATOS'
      when upper(account_name) = 'PREFEITURA DE ARARIPINA'                 then 'ARARIPINA'
      when upper(account_name) = 'PREFEITURA MUNICIPAL DE BOMBINHAS'       then 'BOMBINHAS'
      when upper(account_name) = 'PREFEITURA SANTANA DO IPANEMA'           then 'SANTANA DO IPANEMA'
      when upper(account_name) = 'MUNICIPIO DE IGACI'                      then 'IGUACI'
      when upper(account_name) = 'MUNICIPIO DE FLORIANOPOLIS'              then 'FLORIANOPOLIS'
      when upper(account_name) = 'MUNICIPIO DE POSSE'                      then 'POSSE'
      when upper(account_name) = 'MUNICIPIO DE JOINVILLE'                  then 'JOINVILLE'
      else upper(account_name)
    end as Secretaria,
    c.sent_bacen_at,
    case
      when d.consumer_id is not null and c.sent_bacen_at = d.pri_antecipa  then 'Antecipou dia abertura'
      when d.consumer_id is not null and c.sent_bacen_at < d.pri_antecipa  then 'Antecipou'
      else 'Nunca antecipou'
    end as fl_antecipacao
  from base_docs a
  left join contas b           on a.collaborator_document = b.cpf
  left join abertura c         on b.consumer_id           = c.consumer_id
  left join pri_antecipa d     on b.consumer_id           = d.consumer_id
  left join benefits.companies e on a.company_id          = e.company_id
  left join benefits.accounts  f on e.account_id          = f.account_id
)
select
  date_format(sent_bacen_at, 'yyyy-MM')    as mes,
  date_format(sent_bacen_at, 'yyyy-MM-dd') as dia,
  setor,
  Secretaria                               as secretaria,
  regiao_brasil                            as regiao,
  count(*)                                 as count
from fim
where date_format(sent_bacen_at, 'yyyyMM') > date_format(current_date() - 150, 'yyyyMM')
  and sent_bacen_at  is not null
  and Secretaria     is not null
group by 1, 2, 3, 4, 5
order by 1, 2, 3
"""

# ─── AUTENTICAÇÃO ─────────────────────────────────────────────────────────────
def check_auth():
    result = subprocess.run(
        ["databricks", "auth", "token", "--host", DATABRICKS_HOST, "--profile", DATABRICKS_PROFILE],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        try:
            token_data = json.loads(result.stdout)
            return "access_token" in token_data
        except:
            return False
    return False

def do_login():
    print("🔐 Abrindo browser para autenticação OAuth. Clique em 'Allow' e volte aqui...")
    subprocess.run(
        ["databricks", "auth", "login", "--host", DATABRICKS_HOST, "--profile", DATABRICKS_PROFILE],
        check=True
    )

# ─── QUERY ────────────────────────────────────────────────────────────────────
def run_query():
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState

    w = WorkspaceClient(host=DATABRICKS_HOST, profile=DATABRICKS_PROFILE)

    print("⏳ Submetendo query...")
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=SQL,
        wait_timeout="0s"
    )
    stmt_id = resp.statement_id
    print(f"   statement_id: {stmt_id}")

    elapsed = 0
    while elapsed < 600:
        time.sleep(10)
        elapsed += 10
        s = w.statement_execution.get_statement(stmt_id)
        state = s.status.state
        print(f"   [{elapsed:>3}s] {state}", end="\r", flush=True)

        if state == StatementState.SUCCEEDED:
            print(f"\n✅ Query concluída em {elapsed}s")
            cols = [c.name for c in s.manifest.schema.columns]
            rows = s.result.data_array or []
            data = [dict(zip(cols, r)) for r in rows]
            print(f"   {len(data)} linhas retornadas")
            return data

        elif state in (StatementState.FAILED, StatementState.CANCELED):
            err = s.status.error.message if s.status.error else "desconhecido"
            print(f"\n❌ Query falhou: {err}")
            sys.exit(1)

    print("\n❌ Timeout — query excedeu 600s")
    sys.exit(1)

# ─── GERAR HTML ───────────────────────────────────────────────────────────────
def generate_html(data):
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    data_json = json.dumps(data, ensure_ascii=False)

    # Badge: data mais recente nos dados (campo dia: yyyy-MM-dd → dd/MM/yyyy)
    dias = sorted([r["dia"] for r in data if r.get("dia")], reverse=True)
    if dias:
        y, m, d = dias[0].split("-")
        data_str = f"{d}/{m}/{y}"
    else:
        data_str = datetime.now().strftime("%d/%m/%Y")

    html = template.replace("%%DADOS%%", data_json)
    html = html.replace("Últimos 5 meses", f"Atualizado {data_str}")
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"📄 HTML gerado: {OUTPUT_PATH} | dados até {data_str}")

# ─── UPLOAD ───────────────────────────────────────────────────────────────────
def upload_html():
    """
    Publica o relatório via git push → GitHub Pages.
    O link permanente é: https://felipetrezza.github.io/relatorio-benefits/
    """
    import shutil

    # Copiar para index.html (GitHub Pages serve index.html por padrão)
    index_path = SCRIPT_DIR / "index.html"
    shutil.copy(OUTPUT_PATH, index_path)

    now_str = datetime.now().strftime("%d/%m/%Y %H:%M")

    # Git: pull --rebase primeiro para evitar push rejeitado, depois stage + commit + push
    cmds = [
        ["git", "-C", str(SCRIPT_DIR), "pull", "--rebase", "--autostash", "origin", "main"],
        ["git", "-C", str(SCRIPT_DIR), "add", "index.html"],
        ["git", "-C", str(SCRIPT_DIR), "commit", "-m", f"chore: atualização diária {now_str}"],
        ["git", "-C", str(SCRIPT_DIR), "push", "origin", "main"],
    ]

    for cmd in cmds:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            # "nothing to commit" é OK
            if "nothing to commit" in result.stdout or "nothing to commit" in result.stderr:
                print("   (sem mudanças para commitar — dados iguais)")
                break
            print(f"   ⚠️  {' '.join(cmd[3:])}: {result.stderr.strip()}")
        else:
            if "push" in cmd:
                print(f"✅ Publicado: {GITHUB_PAGES_URL}")
            else:
                print(f"   ✔ {' '.join(cmd[3:])}")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 55)
    print("  Relatório Abertura de Contas — Atualização Diária")
    print(f"  {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("=" * 55)

    # 1. Auth
    print("\n[1/4] Verificando autenticação Databricks...")
    if not check_auth():
        do_login()
        if not check_auth():
            print("❌ Falha na autenticação.")
            sys.exit(1)
    print("   ✅ Autenticado")

    # 2. Query
    print("\n[2/4] Executando query...")
    data = run_query()

    # 3. Gerar HTML
    print("\n[3/4] Gerando HTML...")
    generate_html(data)

    # 4. Upload
    print("\n[4/4] Fazendo upload...")
    upload_html()

    print("\n✅ Concluído!")
    print(f"   Relatório atualizado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}")

if __name__ == "__main__":
    main()
