# SRKES — Airflow Orchestration Pipeline

> **Projet** : Strategic Report Knowledge Extraction System  
> **Certification** : Data Engineer — Simplon  
> **Auteur** : Elizaveta Setkh  

---

## Vue d'ensemble

Ce module implémente l'orchestration complète du pipeline SRKES via **Apache Airflow**.  
Il couvre les compétences **C8, C15, C16, C17, C19, C20, C21** du référentiel Data Engineer.

```
GCS Raw PDFs → [Airflow DAG 1] → Qdrant → GPT-4 → Excel → BigQuery DWH
                [Airflow DAG 2] → Maintenance DWH (dédup, SCD, snapshots)
                [Airflow DAG 3] → Gouvernance Data Lake (catalogue, RGPD)
```

---

## Architecture des DAGs

### DAG 1 — `srkes_ingestion_pipeline` (C8, C15, C19)
**Planification** : tous les jours à 6h UTC

| Tâche | Compétence | Description |
|-------|-----------|-------------|
| `check_api_health` | C15 | Vérifie la disponibilité de l'API FastAPI |
| `detect_new_pdfs` | C19 | Sensor GCS — détecte les nouveaux PDFs |
| `list_and_deduplicate_pdfs` | C8, C15 | Liste les PDFs et exclut les déjà traités (SHA-256) |
| `upload_pdfs_to_qdrant` | C8 | Envoie les PDFs à FastAPI → indexation Qdrant |
| `extract_metrics_to_excel` | C15 | Déclenche l'extraction LLM → génère Excel |
| `upload_excel_to_gcs` | C15, C19 | Archive Excel dans la zone de sortie GCS |
| `load_to_bigquery` | C11, C14, C15 | Charge les métriques dans BigQuery |
| `register_audit_trail` | C15, C19, C21 | Enregistre les hashes dans `file_hashes` |
| `archive_processed_pdfs` | C19 | Déplace les PDFs traités vers `/processed/` |
| `notify_success` | C16 | Notification Slack de fin de pipeline |

### DAG 2 — `srkes_dwh_maintenance` (C16, C17)
**Planification** : tous les jours à 2h UTC

| Tâche | Compétence | Description |
|-------|-----------|-------------|
| `check_fiscalyear_freshness` | C16 | Vérifie la fraîcheur des données (7 jours) |
| `check_no_critical_nulls` | C16 | Contrôle l'absence de NULL sur colonnes clés |
| `detect_and_remove_duplicates` | C16 | Supprime les doublons via DISTINCT |
| `create_dwh_snapshots` | C16 | Snapshots BigQuery de toutes les tables |
| `detect_scd_changes` | C17 | Détecte les révisions de valeurs (SCD) |
| `apply_scd_type1` | C17 | Mise à jour directe des valeurs révisées |
| `apply_scd_type2_metadata` | C17 | Versionnement avec valid_from/valid_to |
| `log_maintenance_metrics` | C16 | Journalise dans `maintenance_log` |
| `notify_maintenance_done` | C16 | Rapport Slack quotidien |

### DAG 3 — `srkes_data_lake_governance` (C20, C21)
**Planification** : tous les dimanches à 3h UTC

| Tâche | Compétence | Description |
|-------|-----------|-------------|
| `inventory_gcs_raw_layer` | C20 | Inventaire complet des objets GCS |
| `inventory_bigquery_dwh` | C20 | Statistiques de toutes les tables BigQuery |
| `update_data_catalog` | C20 | Catalogue centralisé GCS + BigQuery |
| `apply_lifecycle_rules` | C21 | Suppression des objets expirés |
| `check_rgpd_compliance` | C21 | Vérification conformité RGPD |
| `generate_governance_report` | C20, C21 | Rapport hebdomadaire dans BigQuery |

---

## Démarrage rapide (local)

### Prérequis
- Docker Desktop
- Fichier de credentials GCP (`config/gcp_credentials.json`)

### Lancement

```bash
# 1. Cloner le repo et se placer dans le dossier
cd srkes_airflow

# 2. Initialiser Airflow (première fois uniquement)
docker compose run --rm airflow-init

# 3. Démarrer tous les services
docker compose up -d

# 4. Configurer les variables et connexions
docker compose exec airflow-webserver python /opt/airflow/config/setup_airflow.py

# 5. Ouvrir l'interface
open http://localhost:8080  # admin / admin
```

### Variables d'environnement requises

Créer un fichier `.env` à la racine :

```bash
SRKES_GCS_BUCKET=srkes-raw-pdfs
SRKES_BQ_PROJECT=votre-project-id
SRKES_API_HOST=localhost:8000
SRKES_API_TOKEN=votre-token-jwt
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/XXX/YYY/ZZZ
FERNET_KEY=46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLt0nFe2QQ=
```

---

## Déploiement Cloud Composer (GCP)

```bash
# Créer l'environnement Cloud Composer
gcloud composer environments create srkes-airflow \
  --location europe-west1 \
  --image-version composer-2.8.1-airflow-2.9.2 \
  --environment-size small

# Déployer les DAGs
gcloud composer environments storage dags import \
  --environment srkes-airflow \
  --location europe-west1 \
  --source dags/

# Déployer les requirements
gcloud composer environments update srkes-airflow \
  --location europe-west1 \
  --update-pypi-packages-from-file requirements.txt
```

---

## Matrice compétences / DAGs

| Compétence | Intitulé | DAG | Tâche(s) |
|-----------|---------|-----|---------|
| C8 | Automatiser l'extraction | DAG 1 | `list_and_deduplicate_pdfs`, `upload_pdfs_to_qdrant` |
| C15 | Intégrer les ETL | DAG 1 | `extract_metrics_to_excel`, `load_to_bigquery`, `register_audit_trail` |
| C16 | Gérer le DWH | DAG 2 | `check_*`, `create_dwh_snapshots`, `log_maintenance_metrics` |
| C17 | Variations dimensions | DAG 2 | `detect_scd_changes`, `apply_scd_type1`, `apply_scd_type2_metadata` |
| C19 | Infrastructure data lake | DAG 1+3 | `detect_new_pdfs`, `archive_processed_pdfs` |
| C20 | Catalogue des données | DAG 3 | `inventory_*`, `update_data_catalog` |
| C21 | Gouvernance | DAG 1+3 | `register_audit_trail`, `apply_lifecycle_rules`, `check_rgpd_compliance` |

---

## Structure du projet

```
srkes_airflow/
├── dags/
│   ├── srkes_ingestion_dag.py      # DAG 1 — Ingestion & ETL
│   ├── srkes_maintenance_dag.py    # DAG 2 — Maintenance DWH
│   └── srkes_governance_dag.py     # DAG 3 — Gouvernance Data Lake
├── config/
│   ├── setup_airflow.py            # Script d'initialisation
│   ├── cloud_composer_config.yaml  # Config déploiement GCP
│   └── gcp_credentials.json        # Credentials GCP (gitignored)
├── plugins/                        # Operators/Hooks custom (extensible)
├── requirements.txt
├── docker-compose.yml
└── README.md
```
