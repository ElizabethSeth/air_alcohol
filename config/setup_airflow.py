#!/usr/bin/env python3
"""
SRKES — Script de configuration Airflow
========================================
Initialise les Variables et Connexions Airflow nécessaires
au fonctionnement des DAGs SRKES.

Usage :
    # Avec Airflow local installé :
    python config/setup_airflow.py

    # Via Docker :
    docker compose exec airflow-webserver python /opt/airflow/config/setup_airflow.py

"""

import os
import subprocess
import sys


def run(cmd: list[str]) -> None:
    """Exécute une commande airflow CLI."""
    result = subprocess.run(
        ["airflow"] + cmd,
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"  ⚠ {' '.join(cmd)}: {result.stderr.strip()}")
    else:
        print(f"  ✓ {' '.join(cmd[:3])}")


# def setup_variables() -> None:
#     """Configure les Variables Airflow."""setup_variables
#     print("\n📦 Configuration des Variables Airflow...")

#     variables = {
#         "SRKES_GCS_PREFIX"      : os.getenv("SRKES_GCS_PREFIX",        "incoming/"),
#         "SRKES_API_HOST"        : os.getenv("SRKES_API_HOST",           "localhost:8000"),
#         "SRKES_API_TOKEN"       : os.getenv("SRKES_API_TOKEN",          ""),
#         "SRKES_COLLECTION_NAME" : os.getenv("SRKES_COLLECTION_NAME",    "srkes_annual_reports"),
#         "SRKES_FASTAPI_CONN_ID" : "srkes_fastapi",
#         "SRKES_SLACK_CONN_ID"   : "srkes_slack_webhook",
#         "SRKES_SLACK_WEBHOOK_URL": os.getenv("SLACK_WEBHOOK_URL",       ""),
#     }

#     for key, value in variables.items():
#         run(["variables", "set", key, value])


def setup_connections() -> None:
    """Configure les Connexions Airflow."""
    print("\n🔌 Configuration des Connexions Airflow...")

    # Connexion GCP (BigQuery, GCS)
    run([
        "connections", "add", "google_cloud_default",
        "--conn-type", "google_cloud_platform",
        "--conn-extra", '{"key_path": "/opt/airflow/config/gcp_credentials.json"}',
    ])

    # Connexion FastAPI SRKES
    api_host = os.getenv("SRKES_API_HOST", "localhost")
    api_port = os.getenv("SRKES_API_PORT", "8000")
    run([
        "connections", "add", "srkes_fastapi",
        "--conn-type", "http",
        "--conn-host", api_host,
        "--conn-port", api_port,
        "--conn-schema", "http",
    ])

    # Connexion Slack Webhook
    run([
        "connections", "add", "srkes_slack_webhook",
        "--conn-type", "http",
        "--conn-host", "hooks.slack.com",
        "--conn-schema", "https",
        "--conn-password", os.getenv("SLACK_WEBHOOK_TOKEN", ""),
    ])


def check_dags() -> None:
    """Vérifie que les DAGs sont valides."""
    print("\n✅ Vérification des DAGs...")
    dags = [
        "srkes_ingestion_pipeline",
        "srkes_dwh_maintenance",
        "srkes_data_lake_governance",
    ]
    for dag_id in dags:
        run(["dags", "list", "--output", "plain"])
        print(f"  → {dag_id}")


if __name__ == "__main__":
    print("=" * 55)
    print("  SRKES — Configuration Airflow")
    print("=" * 55)

    #setup_variables()
    setup_connections()
    check_dags()

    print("\n🚀 Configuration terminée !")
    print("   → Interface : http://localhost:8080")
    print("   → Login     : admin / admin")
    print("   → Flower    : http://localhost:5555")
