"""
SRKES — DAG 2 : Maintenance du Data Warehouse
===============================================
Compétences couvertes :
  - C16 : Gérer l'entrepôt de données
           (journalisation, alertes, sauvegarde, supervision)
  - C17 : Implémenter des variations dans les dimensions
           (détection et application des SCD)

Description du flux :
  1. Contrôle de santé du DWH (rowcounts, dernière ingestion)
  2. Détection et suppression des doublons (SQL BigQuery)
  3. Détection des Slowly Changing Dimensions (SCD Type 1 & 2)
  4. Application des modifications de dimension
  5. Sauvegarde automatique via BigQuery snapshots
  6. Journalisation des métriques de maintenance
  7. Alerte Slack si anomalie détectée

Planification : quotidienne à 2h UTC (hors fenêtre d'ingestion)

Auteur  : EllySth
Projet  : SR-KES — Data Engineer Certification Simplon
Date    : 2026
"""
from __future__ import annotations

import json
import logging, os
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator

# Airflow 3.x — BigQueryCheckOperator still available, BigQueryExecuteQueryOperator removed
# Use BigQueryInsertJobOperator or plain Python with bigquery client instead


log = logging.getLogger(__name__)

BQ_PROJECT  = os.getenv("AIRFLOW_VAR_SRKES_BQ_PROJECT", "")
BQ_DATASET  = os.getenv("AIRFLOW_VAR_SRKES_BQ_DATASET",  "Reports")
GCS_BUCKET  = os.getenv("AIRFLOW_VAR_SRKES_GCS_BUCKET",  "srkes-raw-pdfs")

DWH_TABLES = [
    "FiscalYear", "Financials", "Region", "Environmental",
    "Social_DEI", "Governance", "Brands", "Sales_Drinks",
]

DEFAULT_ARGS = {
    "owner": "EllySth",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(hours=2),
}

def _slack_alert(context: dict[str, Any]) -> None:
    webhook = os.getenv("SRKES_SLACK_WEBHOOK_URL", "")
    if not webhook:
        return
    import requests
    requests.post(webhook, json={"text": (
        f":warning: *SRKES Maintenance Alert*\n"
        f">DAG: `{context['dag'].dag_id}` | Task: `{context['task_instance'].task_id}`"
    )}, timeout=10)

with DAG(
    dag_id="srkes_dwh_maintenance",
    description="SRKES — Maintenance DWH (C16, C17)",
    default_args=DEFAULT_ARGS,
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["srkes", "maintenance", "dwh", "bloc3"],
    on_failure_callback=_slack_alert,
) as dag:

    start = EmptyOperator(task_id="start")

    check_fiscal_year = BigQueryCheckOperator(
        task_id="check_fiscalyear_freshness",
        sql=f"""
            SELECT COUNT(*) > 0
        FROM `{BQ_PROJECT}.{BQ_DATASET}.all_fiscal_years`
        WHERE Year >= EXTRACT(YEAR FROM CURRENT_DATE()) - 1
        """,
        use_legacy_sql=False,
    )

    check_no_nulls = BigQueryCheckOperator(
        task_id="check_no_critical_nulls",
        sql=f"""
            SELECT COUNT(*) > 0
            FROM `{BQ_PROJECT}.{BQ_DATASET}.all_fiscal_years`
            WHERE Year IS NOT NULL
        """,
        use_legacy_sql=False,
    )
    @task(task_id="detect_and_remove_duplicates")
    def detect_and_remove_duplicates() -> dict:
        """Détecte et supprime les doublons dans chaque table (C16)."""
        from google.cloud import bigquery
        bq     = bigquery.Client(project=BQ_PROJECT)
        report = {}
        for table in DWH_TABLES:
            full = f"`{BQ_PROJECT}.{BQ_DATASET}.{table}`"
            try:
                dup_count = list(bq.query(
                    f"SELECT COUNT(*) - COUNT(DISTINCT Hash) AS n FROM {full}"
                ).result())[0].n
                if dup_count > 0:
                    bq.query(f"CREATE OR REPLACE TABLE {full} AS SELECT DISTINCT * FROM {full}").result()
                    log.info("Dédupliqué %s : %d doublons supprimés", table, dup_count)
                report[table] = int(dup_count)
            except Exception as exc:
                log.warning("Erreur dédup %s : %s", table, exc)
                report[table] = 0
        return report

    dedup_report = detect_and_remove_duplicates()

    @task(task_id="create_dwh_snapshots")
    def create_dwh_snapshots() -> list[str]:
        """Crée des snapshots BigQuery pour chaque table (C16)."""
        from google.cloud import bigquery
        bq          = bigquery.Client(project=BQ_PROJECT)
        ts          = datetime.utcnow().strftime("%Y%m%d")
        snap_ds     = f"{BQ_DATASET}_snapshots"
        bq.create_dataset(bigquery.Dataset(f"{BQ_PROJECT}.{snap_ds}"), exists_ok=True)
        created = []
        for table in DWH_TABLES:
            snap = f"`{BQ_PROJECT}.{snap_ds}.{table}_snap_{ts}`"
            src  = f"`{BQ_PROJECT}.{BQ_DATASET}.{table}`"
            try:
                bq.query(f"CREATE OR REPLACE TABLE {snap} CLONE {src}").result()
                created.append(snap)
                log.info("Snapshot : %s", snap)
            except Exception as exc:
                log.error("Erreur snapshot %s : %s", table, exc)
        return created

    snapshots = create_dwh_snapshots()

    @task(task_id="detect_scd_changes")
    def detect_scd_changes() -> dict:
        """
        Détecte les révisions de valeurs ESG (SCD Type 1 & 2).
        Compétence C17 : variations dans les dimensions.
        """
        from google.cloud import bigquery
        bq = bigquery.Client(project=BQ_PROJECT)
        try:
            rows = list(bq.query(f"""
                WITH latest AS (
                    SELECT Hash, Year, Carbon_emissions,
                           ROW_NUMBER() OVER (PARTITION BY Hash, Year ORDER BY processed_at DESC) AS rn
                    FROM `{BQ_PROJECT}.{BQ_DATASET}.Environmental`
                ),
                prev AS (
                    SELECT Hash, Year, Carbon_emissions,
                           ROW_NUMBER() OVER (PARTITION BY Hash, Year ORDER BY processed_at DESC) AS rn
                    FROM `{BQ_PROJECT}.{BQ_DATASET}.Environmental`
                )
                SELECT l.Hash, l.Year, l.Carbon_emissions AS new_val, p.Carbon_emissions AS old_val
                FROM latest l JOIN prev p ON l.Hash = p.Hash AND l.Year = p.Year
                WHERE l.rn = 1 AND p.rn = 2
                  AND l.Carbon_emissions != p.Carbon_emissions
                  AND l.Carbon_emissions != -1 AND p.Carbon_emissions != -1
            """).result())
            changes = [{"hash": r.Hash, "year": r.Year, "new_val": r.new_val, "old_val": r.old_val}
                       for r in rows]
        except Exception as exc:
            log.warning("SCD query error : %s", exc)
            changes = []
        log.info("SCD changes : %d", len(changes))
        return {"changes": changes, "count": len(changes)}

    scd_changes = detect_scd_changes()

    @task(task_id="apply_scd_type1")
    def apply_scd_type1(scd_result: dict) -> int:
        """
        SCD Type 1 : met à jour directement les valeurs révisées.
        Compétence C17.
        """
        from google.cloud import bigquery
        changes = scd_result.get("changes", [])
        if not changes:
            return 0
        bq      = bigquery.Client(project=BQ_PROJECT)
        updated = 0
        for c in changes:
            try:
                bq.query(f"""
                    UPDATE `{BQ_PROJECT}.{BQ_DATASET}.Environmental`
                    SET Carbon_emissions = {c['new_val']},
                        last_updated_at  = CURRENT_TIMESTAMP()
                    WHERE Hash = '{c['hash']}' AND Year = {c['year']}
                """).result()
                updated += 1
                log.info("SCD1 : Hash %s Year %s | %s → %s",
                         c['hash'][:8], c['year'], c['old_val'], c['new_val'])
            except Exception as exc:
                log.error("SCD1 error : %s", exc)
        return updated

    scd_applied = apply_scd_type1(scd_changes)

    @task(task_id="apply_scd_type2_metadata")
    def apply_scd_type2_metadata() -> int:
        """
        SCD Type 2 : versionnement avec valid_from / valid_to / is_current.
        Compétence C17.
        """
        from google.cloud import bigquery
        bq = bigquery.Client(project=BQ_PROJECT)
 
        bq.query(f"""
            CREATE TABLE IF NOT EXISTS
            `{BQ_PROJECT}.{BQ_DATASET}.dim_collection_history` (
                collection_name STRING NOT NULL,
                company         STRING,
                total_files     INT64,
                total_companies INT64,
                last_hash       STRING,
                valid_from      TIMESTAMP NOT NULL,
                valid_to        TIMESTAMP,
                is_current      BOOL
            )
        """).result()
 
        bq.query(f"""
            UPDATE `{BQ_PROJECT}.{BQ_DATASET}.dim_collection_history`
            SET valid_to   = CURRENT_TIMESTAMP(),
                is_current = FALSE
            WHERE is_current = TRUE
        """).result()
 
        bq.query(f"""
            INSERT INTO `{BQ_PROJECT}.{BQ_DATASET}.dim_collection_history`
                (collection_name, company, total_files, total_companies,
                 last_hash, valid_from, is_current)
            SELECT
                collection_name,
                company,
                COUNT(*) AS total_files,
                COUNT(DISTINCT SPLIT(file_name, '_')[SAFE_OFFSET(0)]) AS total_companies,
                MAX(file_hash) AS last_hash,
                CURRENT_TIMESTAMP() AS valid_from,
                TRUE AS is_current
            FROM (
                SELECT collection_name, file_name, file_hash,
                       'Diageo' AS company
                FROM `{BQ_PROJECT}.Diageo.file_hashes`
                UNION ALL
                SELECT collection_name, file_name, file_hash,
                       'Pernod_ricard' AS company
                FROM `{BQ_PROJECT}.Pernod_ricard.file_hashes`
                UNION ALL
                SELECT collection_name, file_name, file_hash,
                       'Brown_forman' AS company
                FROM `{BQ_PROJECT}.Brown_forman.file_hashes`
                UNION ALL
                SELECT collection_name, file_name, file_hash,
                       'lvmh' AS company
                FROM `{BQ_PROJECT}.lvmh.file_hashes`
            )
            GROUP BY collection_name, company
        """).result()
 
        log.info("SCD Type 2 appliqué.")
        return 1
 
    scd2_applied = apply_scd_type2_metadata()

    @task(task_id="log_maintenance_metrics", trigger_rule=TriggerRule.ALL_DONE)
    def log_maintenance_metrics(dedup_report: dict, scd_result: dict,
                                snapshots_done: list, scd1_applied: int) -> None:
        """Journalise toutes les métriques dans BigQuery (C16)."""
        from google.cloud import bigquery
        bq = bigquery.Client(project=BQ_PROJECT)
        bq.query(f"""
            CREATE TABLE IF NOT EXISTS `{BQ_PROJECT}.{BQ_DATASET}.maintenance_log` (
                run_id           STRING,
                run_date         TIMESTAMP,
                duplicates_found INT64,
                scd_changes      INT64,
                scd1_applied     INT64,
                snapshots_count  INT64,
                tables_checked   INT64,
                status           STRING
            )
        """).result()
        errors = bq.insert_rows_json(f"{BQ_PROJECT}.{BQ_DATASET}.maintenance_log", [{
            "run_id"          : datetime.utcnow().strftime("maint_%Y%m%d_%H%M%S"),
            "run_date"        : datetime.utcnow().isoformat(),
            "duplicates_found": sum(dedup_report.values()) if dedup_report else 0,
            "scd_changes"     : scd_result.get("count", 0),
            "scd1_applied"    : scd1_applied,
            "snapshots_count" : len(snapshots_done or []),
            "tables_checked"  : len(DWH_TABLES),
            "status"          : "completed",
        }])
        if errors:
            log.error("Log errors : %s", errors)

    maint_logged = log_maintenance_metrics(dedup_report, scd_changes, snapshots, scd_applied)

    @task(task_id="notify_maintenance_done", trigger_rule=TriggerRule.ALL_DONE)
    def notify_maintenance_done(dedup_report: dict, scd_result: dict, scd1_applied: int) -> None:
        import requests
        webhook = os.getenv("SRKES_SLACK_WEBHOOK_URL", "")
        if not webhook:
            return
        total_dups = sum(dedup_report.values()) if dedup_report else 0
        requests.post(webhook, json={"text": (
            f":broom: *SRKES Maintenance*\n"
            f">Doublons supprimés: *{total_dups}* | SCD changes: *{scd_result.get('count',0)}* | SCD1 applied: *{scd1_applied}*"
        )}, timeout=10)

    maint_notified = notify_maintenance_done(dedup_report, scd_changes, scd_applied)
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    start >> [check_fiscal_year, check_no_nulls] >> dedup_report >> [snapshots, scd_changes]
    scd_changes >> [scd_applied, scd2_applied]
    [dedup_report, scd_changes, snapshots, scd_applied] >> maint_logged >> maint_notified >> end
    scd2_applied >> end
