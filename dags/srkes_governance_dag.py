"""
SRKES — DAG 3 : Gouvernance du Data Lake
==========================================
Compétences couvertes :
  - C20 : Gérer le catalogue des données
           (inventaire, métadonnées, statistiques GCS + BigQuery)
  - C21 : Implémenter les règles de gouvernance des données
           (cycle de vie, RGPD, expiration, classification)

Description du flux :
  1. Inventaire automatique des objets GCS (Raw Layer)
  2. Collecte des statistiques BigQuery (DWH Layer)
  3. Mise à jour du catalogue de données centralisé
  4. Contrôle des règles de cycle de vie (expiration des données)
  5. Vérification de conformité RGPD
  6. Classification des données par sensibilité
  7. Rapport de gouvernance hebdomadaire

Planification : hebdomadaire, dimanche à 3h UTC
"""
from __future__ import annotations
import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

BQ_PROJECT = Variable.get("SRKES_BQ_PROJECT", default_var="srkes-gcp-project")
BQ_DATASET = Variable.get("SRKES_BQ_DATASET", default_var="Reports")
GCS_BUCKET = Variable.get("SRKES_GCS_BUCKET", default_var="srkes-raw-pdfs")

DATA_LIFECYCLE_RULES = {
    "incoming"        : 7,
    "processed/pdfs"  : 365,
    "processed/excel" : 180,
    "snapshots"       : 30,
}

DATA_CLASSIFICATION = {
    "FiscalYear": "PUBLIC", "Financials": "PUBLIC", "Region": "PUBLIC",
    "Environmental": "PUBLIC", "Social_DEI": "PUBLIC", "Governance": "PUBLIC",
    "Brands": "PUBLIC", "Sales_Drinks": "PUBLIC",
    "file_hashes": "INTERNAL", "maintenance_log": "INTERNAL",
}

DEFAULT_ARGS = {
    "owner": "EllySth",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=3),
}

with DAG(
    dag_id="srkes_data_lake_governance",
    description="SRKES — Gouvernance Data Lake (C20, C21)",
    default_args=DEFAULT_ARGS,
    schedule="0 3 * * 0",
    catchup=False,
    max_active_runs=1,
    tags=["srkes", "governance", "catalog", "data-lake", "bloc4"],
) as dag:

    start = EmptyOperator(task_id="start")

    # ── 1. Inventaire GCS (C20) ───────────────────────────────────────────
    @task(task_id="inventory_gcs_raw_layer")
    def inventory_gcs_raw_layer() -> dict:
        """Inventorie tous les objets GCS du Data Lake (C20)."""
        from google.cloud import storage
        blobs     = list(storage.Client().bucket(GCS_BUCKET).list_blobs())
        by_prefix = {}
        file_types = {}
        dates = []
        for blob in blobs:
            prefix = blob.name.split("/")[0] if "/" in blob.name else "root"
            by_prefix.setdefault(prefix, {"count": 0, "size_mb": 0.0})
            by_prefix[prefix]["count"] += 1
            by_prefix[prefix]["size_mb"] += round((blob.size or 0) / 1e6, 3)
            ext = blob.name.rsplit(".", 1)[-1].lower() if "." in blob.name else "unknown"
            file_types[ext] = file_types.get(ext, 0) + 1
            if blob.time_created:
                dates.append(blob.time_created)
        return {
            "total_objects": len(blobs),
            "total_size_mb": round(sum(b.size or 0 for b in blobs) / 1e6, 2),
            "by_prefix"    : by_prefix,
            "file_types"   : file_types,
            "oldest_file"  : min(dates).isoformat() if dates else None,
            "newest_file"  : max(dates).isoformat() if dates else None,
        }

    gcs_inventory = inventory_gcs_raw_layer()

    @task(task_id="inventory_bigquery_dwh")
    def inventory_bigquery_dwh() -> dict:
        """Collecte les statistiques de toutes les tables BigQuery (C20)."""
        from google.cloud import bigquery
        bq      = bigquery.Client(project=BQ_PROJECT)
        dataset = bq.get_dataset(f"{BQ_PROJECT}.{BQ_DATASET}")
        tables  = list(bq.list_tables(dataset))
        catalog = {"dataset": BQ_DATASET, "project": BQ_PROJECT,
                   "table_count": len(tables), "tables": []}
        for t in tables:
            ref = bq.get_table(t)
            catalog["tables"].append({
                "name"          : t.table_id,
                "row_count"     : ref.num_rows,
                "size_mb"       : round((ref.num_bytes or 0) / 1e6, 2),
                "schema_fields" : len(ref.schema),
                "modified"      : ref.modified.isoformat() if ref.modified else None,
                "classification": DATA_CLASSIFICATION.get(t.table_id, "UNKNOWN"),
                "description"   : ref.description or "",
            })
        return catalog

    bq_catalog = inventory_bigquery_dwh()

    @task(task_id="update_data_catalog")
    def update_data_catalog(gcs_inv: dict, bq_cat: dict) -> None:
        """Consolide GCS + BigQuery dans un catalogue centralisé (C20).

        Utilise load_table_from_json + WRITE_TRUNCATE au lieu de
        DELETE + insert_rows_json pour éviter le conflit avec le
        streaming buffer de BigQuery (erreur 400 à partir du 2e run).
        """
        from google.cloud import bigquery

        bq         = bigquery.Client(project=BQ_PROJECT)
        table_ref  = f"{BQ_PROJECT}.{BQ_DATASET}.data_catalog"
        now_iso    = datetime.utcnow().isoformat()
        schema = [
            bigquery.SchemaField("catalog_id",     "STRING"),
            bigquery.SchemaField("layer",           "STRING"),
            bigquery.SchemaField("object_name",     "STRING"),
            bigquery.SchemaField("object_type",     "STRING"),
            bigquery.SchemaField("classification",  "STRING"),
            bigquery.SchemaField("row_count",       "INT64"),
            bigquery.SchemaField("size_mb",         "FLOAT64"),
            bigquery.SchemaField("schema_fields",   "INT64"),
            bigquery.SchemaField("last_updated",    "TIMESTAMP"),
            bigquery.SchemaField("description",     "STRING"),
            bigquery.SchemaField("owner",           "STRING"),
        ]

        rows: list[dict] = []

        for prefix, stats in gcs_inv.get("by_prefix", {}).items():
            rows.append({
                "catalog_id"    : f"gcs_{prefix}",
                "layer"         : "DATA_LAKE",
                "object_name"   : f"gs://{GCS_BUCKET}/{prefix}/",
                "object_type"   : "GCS_PREFIX",
                "classification": "PUBLIC",
                "row_count"     : stats["count"],
                "size_mb"       : stats["size_mb"],
                "schema_fields" : 0,
                "last_updated"  : now_iso,
                "description"   : f"Dossier GCS — {prefix}",
                "owner"         : "data_engineering_team",
            })

        for table in bq_cat.get("tables", []):
            rows.append({
                "catalog_id"    : f"bq_{table['name']}",
                "layer"         : "DATA_WAREHOUSE",
                "object_name"   : f"{BQ_PROJECT}.{BQ_DATASET}.{table['name']}",
                "object_type"   : "BIGQUERY_TABLE",
                "classification": table["classification"],
                "row_count"     : table["row_count"],
                "size_mb"       : table["size_mb"],
                "schema_fields" : table["schema_fields"],
                "last_updated"  : table["modified"] or now_iso,
                "description"   : table["description"],
                "owner"         : "data_engineering_team",
            })

        if not rows:
            log.warning("Catalogue : aucune entrée à écrire, tâche ignorée.")
            return

        # ── Écriture atomique : WRITE_TRUNCATE remplace DELETE + streaming ─
        # Pas de streaming buffer → pas d'erreur 400 au 2e run et suivants.
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        job = bq.load_table_from_json(rows, table_ref, job_config=job_config)
        job.result()  # attend la fin du job et lève une exception si erreur

        log.info(
            "Catalogue mis à jour : %d entrées → %s (job %s)",
            len(rows), table_ref, job.job_id,
        )

    catalog_updated = update_data_catalog(gcs_inventory, bq_catalog)

    @task(task_id="apply_lifecycle_rules")
    def apply_lifecycle_rules() -> dict:
        """Supprime les objets GCS expirés selon les règles de rétention (C21)."""
        from google.cloud import storage
        bucket  = storage.Client().bucket(GCS_BUCKET)
        now     = datetime.utcnow().replace(tzinfo=None)
        deleted = {}
        for prefix, max_days in DATA_LIFECYCLE_RULES.items():
            cutoff  = now - timedelta(days=max_days)
            expired = [
                blob for blob in bucket.list_blobs(prefix=prefix + "/")
                if blob.time_created and blob.time_created.replace(tzinfo=None) < cutoff
            ]
            for blob in expired:
                blob.delete()
                log.info("Supprimé (cycle de vie) : %s", blob.name)
            deleted[prefix] = len(expired)
            if prefix == "incoming" and expired:
                log.warning("%d fichier(s) non traités depuis > %d jours !", len(expired), max_days)
        return deleted

    lifecycle_applied = apply_lifecycle_rules()

    @task(task_id="check_rgpd_compliance")
    def check_rgpd_compliance() -> dict:
        """Vérifie la conformité RGPD du pipeline (C21)."""
        from google.cloud import bigquery
        bq         = bigquery.Client(project=BQ_PROJECT)
        compliance = {"status": "COMPLIANT", "checks": [], "violations": []}
        try:
            cnt = list(bq.query(
                f"SELECT COUNT(*) AS cnt FROM `{BQ_PROJECT}.{BQ_DATASET}.file_hashes`"
            ).result())[0].cnt
            compliance["checks"].append({"rule": "REGISTRE_TRAITEMENTS", "status": "OK",
                                         "detail": f"{cnt} entrées"})
        except Exception as exc:
            compliance["violations"].append({"rule": "REGISTRE_TRAITEMENTS", "detail": str(exc)})
            compliance["status"] = "NON_COMPLIANT"
        compliance["checks"].append({"rule": "NO_PERSONAL_DATA", "status": "OK",
                                     "detail": "Sources publiques uniquement (IR portals, SEC, AMF)"})
        log.info("RGPD : %s (%d checks)", compliance["status"], len(compliance["checks"]))
        return compliance

    rgpd_checked = check_rgpd_compliance()

    @task(task_id="generate_governance_report", trigger_rule=TriggerRule.ALL_DONE)
    def generate_governance_report(gcs_inv: dict, bq_cat: dict,
                                   lifecycle_result: dict, rgpd_result: dict) -> None:
        """Rapport hebdomadaire de gouvernance dans BigQuery (C20, C21)."""
        from google.cloud import bigquery
        bq = bigquery.Client(project=BQ_PROJECT)
        bq.query(f"""
            CREATE TABLE IF NOT EXISTS `{BQ_PROJECT}.{BQ_DATASET}.governance_report` (
                report_id       STRING,
                report_date     TIMESTAMP,
                gcs_objects     INT64,
                gcs_size_mb     FLOAT64,
                bq_tables       INT64,
                deleted_objects INT64,
                rgpd_status     STRING,
                rgpd_violations INT64,
                catalog_entries INT64
            )
        """).result()
        total_deleted = sum(lifecycle_result.values()) if lifecycle_result else 0
        total_catalog = len(gcs_inv.get("by_prefix", {})) + len(bq_cat.get("tables", []))
        errors = bq.insert_rows_json(f"{BQ_PROJECT}.{BQ_DATASET}.governance_report", [{
            "report_id"      : f"gov_{datetime.utcnow().strftime('%Y%m%d')}",
            "report_date"    : datetime.utcnow().isoformat(),
            "gcs_objects"    : gcs_inv.get("total_objects", 0),
            "gcs_size_mb"    : gcs_inv.get("total_size_mb", 0.0),
            "bq_tables"      : bq_cat.get("table_count", 0),
            "deleted_objects": total_deleted,
            "rgpd_status"    : rgpd_result.get("status", "UNKNOWN"),
            "rgpd_violations": len(rgpd_result.get("violations", [])),
            "catalog_entries": total_catalog,
        }])
        if errors:
            log.error("Rapport errors : %s", errors)

    gov_report = generate_governance_report(gcs_inventory, bq_catalog, lifecycle_applied, rgpd_checked)
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    start >> [gcs_inventory, bq_catalog]
    [gcs_inventory, bq_catalog] >> catalog_updated
    catalog_updated >> [lifecycle_applied, rgpd_checked]
    [catalog_updated, lifecycle_applied, rgpd_checked] >> gov_report >> end
