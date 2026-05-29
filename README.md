# FinDataLake — Datalake Financier CAC 40

![Lint](https://github.com/{username}/findatalake/actions/workflows/ci.yml/badge.svg?label=lint)
![Tests](https://github.com/{username}/findatalake/actions/workflows/ci.yml/badge.svg?label=test)
![Docker Build](https://github.com/{username}/findatalake/actions/workflows/ci.yml/badge.svg?label=docker-build)

Plateforme de datalake financier pour le suivi en temps réel des 40 entreprises du CAC 40. Le projet ingère des données boursières via yfinance et des données fondamentales depuis un fichier CSV, les transforme en Parquet via une zone staging, expose une API REST FastAPI, et applique un modèle Isolation Forest non supervisé pour détecter les anomalies de marché.

---

## Architecture

Le projet suit une architecture **Médaillon** (RAW → Staging → Curated) hébergée sur Azure Blob Storage. L'orchestration est assurée par Airflow avec des DAGs dédiés à chaque couche. Une API FastAPI expose les données à chaque étape et permet de déclencher les pipelines manuellement. La détection d'anomalies est réalisée avec un modèle Isolation Forest entraîné sur les données staging et servi via MLflow.

| Couche | Technologie | Rôle |
|---|---|---|
| **RAW** | Azure Blob Storage | Stockage brut des JSON yfinance et du CSV fondamentaux |
| **Staging** | Azure Blob Storage + Parquet | Données nettoyées, typées, enrichies (daily_return, moving_avg_20) |
| **Curated** | DuckDB | Couche analytique requêtable, agrégations sur fact_market_daily |
| **Orchestration** | Apache Airflow 2.9 | DAGs planifiés pour ingestion, transformation, ML et export |
| **API** | FastAPI + Uvicorn | Gateway REST exposant toutes les couches et les endpoints d'ingestion |
| **ML** | scikit-learn (Isolation Forest) + MLflow | Détection d'anomalies non supervisée, tracking des expériences |

---

## Prérequis

- **Docker Desktop** installé et lancé (version 24+ recommandée)
- **Python 3.11+** pour exécuter les tests en local hors Docker
- **Un compte Azure** avec Azure Blob Storage configuré (3 containers : `raw`, `staging`, `models`)
- **Une clé API Alpha Vantage** gratuite : https://www.alphavantage.co/support/#api-key

---

## Installation et lancement

### 1. Cloner le repo

```bash
git clone https://github.com/{username}/findatalake.git
cd findatalake
```

### 2. Configurer les variables d'environnement

```bash
cp .env.example .env
# Éditer .env et renseigner :
# - AZURE_STORAGE_CONNECTION_STRING  (chaîne de connexion Azure Blob Storage)
# - ALPHA_VANTAGE_KEY                (clé API Alpha Vantage)
```

### 3. Lancer la stack complète

```bash
docker compose up -d
```

Le démarrage complet prend environ 60 à 90 secondes (initialisation de la base Airflow incluse).

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| API (Swagger) | http://localhost:8000/docs | — |
| MLflow UI | http://localhost:5000 | — |

### 4. Lancer le pipeline manuellement (premier run)

```bash
# Ingestion des cours boursiers CAC 40 via yfinance
docker compose exec api python -m ingestion.api_ingestor

# Ingestion du fichier de données fondamentales
docker compose exec api python -m ingestion.file_ingestor

# Transformation RAW → Staging (Parquet)
docker compose exec api python -m transformation.spark_staging

# Entraînement du modèle Isolation Forest
docker compose exec api python -m ml.train

# Scoring et détection d'anomalies
docker compose exec api python -m ml.predict
```

---

## Endpoints API

| Method | Endpoint | Description | Exemple |
|---|---|---|---|
| GET | `/health` | État des services (Azure, DuckDB) | `curl http://localhost:8000/health` |
| GET | `/stats` | Nombre de blobs et taille par container | `curl http://localhost:8000/stats` |
| GET | `/raw` | Liste des blobs dans la zone RAW | `curl "http://localhost:8000/raw?prefix=api/&limit=10"` |
| GET | `/staging` | Liste des Parquet dans la zone Staging | `curl "http://localhost:8000/staging?zone=market_data"` |
| GET | `/curated` | Requête sur fact_market_daily (DuckDB) | `curl "http://localhost:8000/curated?ticker=MC.PA&limit=50"` |
| POST | `/ingest` | Ingestion séquentielle des tickers | voir ci-dessous |
| POST | `/ingest_fast` | Ingestion parallèle (async + ThreadPool) | voir ci-dessous |

```bash
# GET /health
curl http://localhost:8000/health

# GET /stats
curl http://localhost:8000/stats

# GET /raw — 10 derniers blobs sous le préfixe api/
curl "http://localhost:8000/raw?prefix=api/&limit=10"

# GET /staging — blobs market_data
curl "http://localhost:8000/staging?zone=market_data&limit=20"

# GET /curated — données LVMH
curl "http://localhost:8000/curated?ticker=MC.PA&limit=100"

# POST /ingest — ingestion séquentielle de 2 tickers
curl -X POST http://localhost:8000/ingest \
     -H "Content-Type: application/json" \
     -d '{"tickers": ["MC.PA", "TTE.PA"]}'

# POST /ingest_fast — ingestion parallèle de tous les CAC 40
curl -X POST http://localhost:8000/ingest_fast \
     -H "Content-Type: application/json" \
     -d '{}'
```

---

## Tests

```bash
# Tests en local (hors Docker)
pip install -r requirements.txt
pytest tests/ -v

# Tests dans le conteneur Docker
docker compose exec api pytest tests/ -v

# Avec rapport de couverture
pytest tests/ -v --cov=. --cov-report=term-missing
```

---

## Structure du projet

```
findatalake/
├── .github/
│   └── workflows/
│       └── ci.yml                  # Pipeline CI : lint → test → docker-build
├── airflow/
│   └── dags/
│       ├── dag_ingest_api.py       # DAG ingestion yfinance toutes les 2h (semaine)
│       ├── dag_ingest_file.py      # DAG upload CSV fondamentaux (lundi 7h)
│       ├── dag_transform_staging.py# DAG transformation RAW → Staging (20h semaine)
│       ├── dag_ml_scoring.py       # DAG train + score Isolation Forest (21h semaine)
│       └── dag_export.py           # DAG rapport hebdomadaire via /stats (lundi 7h)
├── ingestion/
│   ├── api_ingestor.py             # Récupération yfinance → JSON → Azure RAW
│   └── file_ingestor.py            # Upload CSV fondamentaux → Azure RAW
├── transformation/
│   └── spark_staging.py            # Lecture RAW → nettoyage pandas → Parquet Staging
├── ml/
│   ├── train.py                    # Entraînement Isolation Forest → Azure models/
│   └── predict.py                  # Scoring anomalies sur données Staging
├── api/
│   ├── main.py                     # App FastAPI, CORS, routers, startup event
│   └── routers/
│       ├── health.py               # GET /health — état Azure et DuckDB
│       ├── stats.py                # GET /stats — métriques par container
│       ├── raw.py                  # GET /raw — liste blobs zone RAW
│       ├── staging.py              # GET /staging — liste Parquet zone Staging
│       ├── curated.py              # GET /curated — requêtes DuckDB
│       └── ingest.py               # POST /ingest et /ingest_fast
├── tests/
│   ├── test_ingestors.py           # Tests APIIngestor et FileIngestor (9 tests)
│   ├── test_transformations.py     # Tests StagingTransformer (5 tests)
│   └── test_api.py                 # Tests endpoints FastAPI via TestClient (8 tests)
├── data/
│   └── cac40_fundamentals.csv      # Données fondamentales 2024-2025 des 40 entreprises
├── .env.example                    # Template des variables d'environnement
├── .gitignore                      # Exclusions git (.env, __pycache__, *.duckdb…)
├── docker-compose.yml              # Stack complète : Postgres, Airflow, API, MLflow
├── Dockerfile                      # Image Python 3.11-slim pour le service api
├── pyproject.toml                  # Config Black (line-length=100) et pytest
└── requirements.txt                # Dépendances Python du projet
```

---

## Choix techniques

### Azure Blob Storage (zone RAW) plutôt qu'Elasticsearch

Azure Blob Storage est adapté au stockage de données financières structurées au format JSON et CSV. Son partitionnement Hive (`year=/month=/day=/`) permet des lectures sélectives sans scanner tout le container. Elasticsearch aurait été surdimensionné pour ce cas d'usage : son moteur d'indexation full-text n'apporte pas de valeur sur des données numériques, et son coût opérationnel est significativement plus élevé.

### DuckDB (zone Curated) plutôt que PostgreSQL

DuckDB est un moteur analytique in-process, sans serveur à administrer, nativement capable de lire des fichiers Parquet directement depuis le disque ou depuis Azure. Pour des requêtes analytiques sur des séries temporelles financières (agrégations, fenêtres glissantes), ses performances sont comparables à celles de PostgreSQL avec zéro infrastructure. Le coût opérationnel est nul.

### Isolation Forest pour la détection d'anomalies

Isolation Forest est un algorithme non supervisé particulièrement adapté aux séries temporelles financières : il ne nécessite pas de données labellisées (anomalies inconnues a priori), résiste aux données multidimensionnelles corrélées (close, volume, daily_return, moving_avg_20), et s'entraîne efficacement sur de grands volumes. Le paramètre `contamination=0.05` modélise l'hypothèse que 5 % des observations de marché sont anormales, ce qui est cohérent avec les données historiques CAC 40.

### `/ingest_fast` — stratégie async + ThreadPoolExecutor

yfinance est une bibliothèque synchrone et bloquante. Dans un endpoint FastAPI `async def`, les appels bloquants doivent être délégués à un thread pool via `asyncio.get_running_loop().run_in_executor(None, func, arg)`. `asyncio.gather()` lance ensuite tous les tickers en parallèle dans des threads distincts. L'opération étant I/O-bound (réseau Yahoo Finance + upload Azure), la parallélisation est quasi-linéaire jusqu'à la limite du pool de threads.

---

## Performances `/ingest` vs `/ingest_fast`

| Batch size | `/ingest` (séquentiel) | `/ingest_fast` (parallèle) | Gain estimé |
|---|---|---|---|
| 1 ticker | ~2 s | ~2 s | ~0 % |
| 10 tickers | ~20 s | ~6 s | ~70 % |
| 40 tickers | ~80 s | ~12 s | ~85 % |

Le gain est quasi-nul pour 1 ticker (overhead du thread pool), mais croît rapidement car chaque ticker attend ~2 s de réponse réseau. En mode parallèle, ces 2 s sont partagées sur l'ensemble du batch — la durée totale est dominée par le ticker le plus lent plutôt que par la somme de tous.

---

## Auteur

**Rody Brayan DAMA SOMO**
EFREI Paris — Master Data & IA — 2025-2026
[LinkedIn](https://www.linkedin.com/in/rody-brayan-dama-somo/)
