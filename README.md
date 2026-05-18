# bookwise

> Discover your next favourite read with personalised book recommendations.

`bookwise` is a Flask web application that serves book recommendations computed by an Apache Airflow pipeline. Users register, log in, and browse pre-computed association-rule recommendations — no ML runs on the web request path.

## Architecture

```
┌─────────────┐     reads       ┌───────────────────┐
│  Flask app  │ ──────────────► │  MongoDB (Bookify) │
│  port 5000  │                 │  collections:      │
└─────────────┘                 │  • users           │
                                │  • books           │
┌─────────────────────────┐     │  • ratings         │
│  Airflow (@daily)       │     │  • Recommend ◄──── │
│  recommendation_pipeline│ ──► │                    │
│  extract → compute      │     └───────────────────┘
│  → load → cleanup       │
└─────────────────────────┘
      Celery worker + Redis broker
      Metadata: PostgreSQL
```

The Airflow DAG runs the Apriori market-basket algorithm over user ratings and writes association rules to the `Recommend` collection. Flask reads from that collection — it never runs ML itself.

## Features

- User registration and login with bcrypt password hashing
- Personalised recommendation feed (pre-computed association rules)
- Book catalogue with individual product pages
- Responsive Bootstrap UI

## Tech Stack

| Layer | Technology |
|---|---|
| Web | Python 3.11 / Flask |
| Database | MongoDB |
| Pipeline | Apache Airflow 2.9 (CeleryExecutor) |
| Broker | Redis |
| Airflow metadata | PostgreSQL |
| ML | mlxtend Apriori |
| Containerisation | Docker Compose |

## Quick Start (Docker Compose)

### 1. Clone and configure

```bash
git clone https://github.com/Vedanshu7/bookwise
cd bookwise
cp .env.example .env
```

On Linux, also set the Airflow UID:

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
```

### 2. Start all services

```bash
docker compose up --build
```

This starts: Flask (5000), MongoDB (27017), Airflow webserver (8080), scheduler, worker, Redis, and PostgreSQL.

### 3. Seed the database

Wait for the worker to become healthy, then:

```bash
docker compose exec airflow-worker \
    python /opt/airflow/scripts/seed_ratings.py
```

### 4. Run the pipeline

Open the Airflow UI at `http://localhost:8080` (user: `airflow`, password: `airflow`).  
Enable and trigger the `recommendation_pipeline` DAG, or wait for the daily schedule.

### 5. Open the app

`http://localhost:5000` — register an account and visit `/recommend`.

## Local Development (no Docker)

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
export MONGO_URI=mongodb://localhost:27017/Bookify
export SECRET_KEY=dev-secret
python src/app.py
```

Requires a local MongoDB instance. Airflow is not available in this mode; seed and run the pipeline via Docker to populate the `Recommend` collection.

## Project Structure

```
bookwise/
├── src/
│   └── app.py                  # Flask routes and app config
├── airflow/
│   ├── Dockerfile              # Airflow image (extends apache/airflow)
│   ├── requirements.txt        # Airflow-side dependencies (mlxtend, pandas)
│   ├── dags/
│   │   └── recommendation_pipeline.py   # Apriori DAG (extract→compute→load→cleanup)
│   └── scripts/
│       └── seed_ratings.py     # One-shot script to populate books + ratings
├── templates/                  # Jinja2 HTML templates
├── static/                     # CSS, JS, images
├── Dockerfile                  # Flask app image
├── docker-compose.yml          # Full-stack composition
├── requirements.txt            # Flask app dependencies
├── .env.example                # Environment variable template
└── README.md
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `MONGO_URI` | `mongodb://mongo:27017/Bookify` | MongoDB connection string |
| `SECRET_KEY` | `change-me-in-production` | Flask session secret |
| `FLASK_DEBUG` | `false` | Enable Flask debug mode |
| `AIRFLOW_UID` | `50000` | UID for Airflow containers (Linux: `id -u`) |
| `PIPELINE_MIN_SUPPORT` | `0.01` | Minimum support threshold for Apriori |
| `PIPELINE_MIN_LIFT` | `1.0` | Minimum lift threshold for association rules |

## License

[MIT](LICENSE)
