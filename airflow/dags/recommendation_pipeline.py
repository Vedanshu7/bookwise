"""
recommendation_pipeline — Airflow DAG.

Computes Apriori association rules from user ratings and writes them to the
MongoDB Recommend collection so the Flask /recommend route reads pre-computed
results instead of running ML on every request.

Pipeline stages:
    extract   — copy ratings from MongoDB into a staging collection
    compute   — build basket matrix, run Apriori, write rules to staging
    load      — atomically replace the live Recommend collection
    cleanup   — drop staging collections
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ============================================================
# Configuration.
# ============================================================

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/Bookify")
DB_NAME = "Bookify"

COL_RATINGS = "ratings"
COL_RECOMMEND = "Recommend"
COL_STAGING_RATINGS = "_staging_ratings"
COL_STAGING_RULES = "_staging_rules"

MIN_SUPPORT = float(os.getenv("PIPELINE_MIN_SUPPORT", "0.01"))
MIN_LIFT = float(os.getenv("PIPELINE_MIN_LIFT", "1.0"))

# ============================================================
# Task callables.
# ============================================================


def extract(**context) -> None:
    """Copy the ratings collection into a staging collection.

    Pushes the document count to XCom so downstream tasks can short-circuit
    if no data is available without opening an additional DB connection.
    """
    import pymongo

    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]

    ratings = list(db[COL_RATINGS].find({}, {"_id": 0}))
    if not ratings:
        client.close()
        raise ValueError(
            f"Collection '{COL_RATINGS}' is empty. "
            "Seed the database before running the pipeline."
        )

    db[COL_STAGING_RATINGS].drop()
    db[COL_STAGING_RATINGS].insert_many(ratings)
    client.close()

    context["ti"].xcom_push(key="rating_count", value=len(ratings))
    print(f"Staged {len(ratings)} ratings.")


def compute(**context) -> None:
    """Build a user-book basket matrix, run Apriori, and write rules to staging.

    Expected rating document format:
        {"user": "<user_id>", "book": "<title>", "rating": <1-5>}

    Output rule document format:
        {"antecedents": "<title>", "consequents": "<title>",
         "confidence": <float>, "lift": <float>, "Price_C": <float>}
    """
    import pandas as pd
    import pymongo
    from mlxtend.frequent_patterns import apriori, association_rules

    count = context["ti"].xcom_pull(key="rating_count", task_ids="extract")
    if not count:
        raise ValueError("XCom key 'rating_count' missing — extract task may have failed.")

    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]

    ratings = list(db[COL_STAGING_RATINGS].find({}, {"_id": 0}))
    df = pd.DataFrame(ratings)

    # Build one-hot basket: rows = users, columns = book titles.
    basket = (
        df.groupby(["user", "book"])["rating"]
        .sum()
        .unstack(fill_value=0)
        .map(lambda v: 1 if v > 0 else 0)
        .astype(bool)
    )

    frequent_itemsets = apriori(basket, min_support=MIN_SUPPORT, use_colnames=True)
    if frequent_itemsets.empty:
        client.close()
        raise ValueError(
            f"No frequent itemsets found with min_support={MIN_SUPPORT}. "
            "Lower PIPELINE_MIN_SUPPORT and retry."
        )

    rules = association_rules(
        frequent_itemsets, metric="lift", min_threshold=MIN_LIFT
    )

    # Keep only single-antecedent, single-consequent rules to match the
    # template filter: {% if i['antecedents'] == request.args.get('a') %}
    rules = rules[
        rules["antecedents"].apply(len) == 1
    ][["antecedents", "consequents", "lift", "confidence"]]

    rules["antecedents"] = rules["antecedents"].apply(lambda s: next(iter(s)))
    rules["consequents"] = rules["consequents"].apply(lambda s: next(iter(s)))

    # Enrich with price from the books catalog if it exists.
    prices = {
        b["title"]: b.get("price", 0.0)
        for b in db.books.find({}, {"_id": 0, "title": 1, "price": 1})
    }
    rules["Price_C"] = rules["consequents"].map(lambda t: prices.get(t, 0.0))

    records = rules.to_dict("records")
    db[COL_STAGING_RULES].drop()
    db[COL_STAGING_RULES].insert_many(records)
    client.close()

    context["ti"].xcom_push(key="rule_count", value=len(records))
    print(f"Computed {len(records)} association rules.")


def load(**context) -> None:
    """Atomically replace the live Recommend collection with staged rules."""
    import pymongo

    rule_count = context["ti"].xcom_pull(key="rule_count", task_ids="compute")
    if not rule_count:
        raise ValueError("XCom key 'rule_count' missing — compute task may have failed.")

    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]

    rules = list(db[COL_STAGING_RULES].find({}, {"_id": 0}))
    db[COL_RECOMMEND].drop()
    db[COL_RECOMMEND].insert_many(rules)
    client.close()

    print(f"Loaded {rule_count} rules into '{COL_RECOMMEND}'.")


def cleanup(**_) -> None:
    """Drop both staging collections after a successful pipeline run."""
    import pymongo

    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    db[COL_STAGING_RATINGS].drop()
    db[COL_STAGING_RULES].drop()
    client.close()
    print("Staging collections dropped.")


# ============================================================
# DAG definition.
# ============================================================

default_args = {
    "owner": "bookwise",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
}

with DAG(
    "recommendation_pipeline",
    default_args=default_args,
    description="Daily Apriori pipeline: ratings → rules → Recommend collection.",
    schedule_interval="@daily",
    catchup=False,
    tags=["bookwise", "recommendations"],
) as dag:

    t_extract = PythonOperator(task_id="extract", python_callable=extract)
    t_compute = PythonOperator(task_id="compute", python_callable=compute)
    t_load    = PythonOperator(task_id="load",    python_callable=load)
    t_cleanup = PythonOperator(task_id="cleanup", python_callable=cleanup)

    t_extract >> t_compute >> t_load >> t_cleanup
