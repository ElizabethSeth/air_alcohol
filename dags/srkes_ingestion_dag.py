"""
SRKES — DAG 1 : Ingestion & Extraction Pipeline
================================================
Compétences couvertes :
  - C8  : Automatisation de l'extraction de données (GCS → FastAPI → Qdrant)
  - C15 : Intégration ETL en entrée et en sortie du DWH
  - C19 : Intégration des composants d'infrastructure du data lake

Description du flux :
  1. Détecte les nouveaux fichiers PDF dans le bucket GCS (Raw Layer)
  2. Déduplique via les hashes SHA-256 stockés dans BigQuery
  3. Appelle l'API FastAPI /upload_pdfs pour indexer dans Qdrant
  4. Déclenche l'extraction LLM via /return_excel
  5. Charge les métriques structurées dans BigQuery (/upload_to_bigquery)
  6. Enregistre le hash dans la table d'audit file_hashes
  7. Envoie une notification Slack/email sur succès ou échec

Auteur  : Elly
Projet  : SR-KES — Data Engineer Certification Simplon
Date    : 2026
"""


from __future__ import annotations
import hashlib, logging, os
from datetime import datetime, timedelta
from typing import Any
 
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
 
log = logging.getLogger(__name__)
 
# ── Read directly from .env (AIRFLOW_VAR_ prefix) ─────────────────────────
GCS_BUCKET      = os.getenv("AIRFLOW_VAR_SRKES_GCS_BUCKET",     "srkes-raw-pdfs")
GCS_PREFIX      = os.getenv("AIRFLOW_VAR_SRKES_GCS_PREFIX",      "incoming/")
BQ_PROJECT      = os.getenv("AIRFLOW_VAR_SRKES_BQ_PROJECT",      "srkes-gcp-project")
BQ_DATASET      = os.getenv("AIRFLOW_VAR_SRKES_BQ_DATASET",      "Reports")
COLLECTION_NAME = os.getenv("AIRFLOW_VAR_SRKES_COLLECTION_NAME", "srkes_annual_reports")
SLACK_WEBHOOK   = os.getenv("AIRFLOW_VAR_SRKES_SLACK_WEBHOOK_URL", "")

 
DEFAULT_ARGS = {
    "owner"            : "EllySth",
    "depends_on_past"  : False,
    "start_date"       : datetime(2026, 1, 1),
    "email_on_failure" : False,
    "retries"          : 2,
    "retry_delay"      : timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=60),
}
 
 
def _compute_sha256(data: bytes) -> str:
    """Hash SHA-256 pour déduplication et traçabilité (C15, C19)."""
    h = hashlib.sha256()
    for i in range(0, len(data), 8192):
        h.update(data[i: i + 8192])
    return h.hexdigest()
 
 
def _slack_alert(context: dict[str, Any]) -> None:
    """Alerte Slack en cas d'échec (optionnel)."""
    webhook = os.getenv("AIRFLOW_VAR_SRKES_SLACK_WEBHOOK_URL", "")
    if not webhook:
        return
    import requests
    requests.post(webhook, json={"text": (
        f":red_circle: *SRKES Failed*\n"
        f">DAG: `{context['dag'].dag_id}` | Task: `{context['task_instance'].task_id}`"
    )}, timeout=10)
 
 
with DAG(
    dag_id="srkes_ingestion_pipeline",
    description="SRKES — Ingestion & ETL (C8, C15, C19)",
    default_args=DEFAULT_ARGS,
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["srkes", "ingestion", "etl", "bloc2", "bloc4"],
    on_failure_callback=_slack_alert,
) as dag:
 
    start = EmptyOperator(task_id="start")
 
    @task(task_id="list_and_deduplicate_pdfs")
    def list_and_deduplicate_pdfs() -> list[dict]:
        """Liste les PDFs GCS et exclut les déjà traités via SHA-256."""
        from google.cloud import bigquery, storage
 
        gcs_client = storage.Client()
        bq_client  = bigquery.Client(project=BQ_PROJECT)
 
        try:
            known_hashes = {
                row.file_hash for row in bq_client.query(
                    f"SELECT DISTINCT file_hash FROM `{BQ_PROJECT}.{BQ_DATASET}.file_hashes`"
                ).result()
            }
            log.info("Hashes connus : %d", len(known_hashes))
        except Exception:
            log.warning("Table file_hashes introuvable — premier run.")
            known_hashes = set()
 
        new_files = []
        for blob in gcs_client.bucket(GCS_BUCKET).list_blobs(prefix=GCS_PREFIX):
            if not blob.name.endswith(".pdf"):
                continue
            file_hash = _compute_sha256(blob.download_as_bytes())
            if file_hash not in known_hashes:
                new_files.append({
                    "gcs_path" : f"gs://{GCS_BUCKET}/{blob.name}",
                    "file_name": blob.name.split("/")[-1],
                    "file_hash": file_hash,
                })
                log.info("Nouveau fichier : %s", blob.name)
 
        if not new_files:
            raise AirflowSkipException("Aucun nouveau PDF dans GCS.")
 
        log.info("%d fichier(s) à traiter.", len(new_files))
        return new_files
 
    files_to_process = list_and_deduplicate_pdfs()
 
    @task(task_id="register_audit_trail")
    def register_audit_trail(files: list[dict]) -> None:
        """Enregistre les hashes SHA-256 dans BigQuery pour traçabilité."""
        from google.cloud import bigquery

        if not files:
            return

        bq   = bigquery.Client(project=BQ_PROJECT)

        bq.query(f"""
            CREATE TABLE IF NOT EXISTS
            `{BQ_PROJECT}.{BQ_DATASET}.file_hashes` (
                collection_name STRING,
                file_name       STRING,
                file_hash       STRING,
                processed_at    TIMESTAMP
            )
        """).result()

        rows = [
            {
                "collection_name": COLLECTION_NAME,
                "file_name"      : f["file_name"],
                "file_hash"      : f["file_hash"],
                "processed_at"   : datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            }
            for f in files
        ]
        errors = bq.insert_rows_json(
            f"{BQ_PROJECT}.{BQ_DATASET}.file_hashes", rows
        )
        if errors:
            raise RuntimeError(f"BigQuery errors: {errors}")
        log.info("Audit trail enregistré : %d fichier(s)", len(rows))

    audit_registered = register_audit_trail(files_to_process)

    
    @task(task_id="archive_processed_pdfs")
    def archive_processed_pdfs(files: list[dict]) -> None:
        """Déplace les PDFs traités dans la zone archivée du Data Lake."""
        from google.cloud import storage
 
        bucket = storage.Client().bucket(GCS_BUCKET)
        ts     = datetime.utcnow().strftime("%Y/%m/%d")
 
        for fi in files:
            src = GCS_PREFIX + fi["file_name"]
            dst = f"processed/pdfs/{ts}/{fi['file_name']}"
            try:
                bucket.copy_blob(bucket.blob(src), bucket, dst)
                bucket.blob(src).delete()
                log.info("Archivé : %s → %s", src, dst)
            except Exception as exc:
                log.error("Erreur archivage %s : %s", fi["file_name"], exc)
 
    pdfs_archived = archive_processed_pdfs(files_to_process)

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)
 
    start >> files_to_process >> [audit_registered, pdfs_archived]
    audit_registered >> end
    pdfs_archived >> end


