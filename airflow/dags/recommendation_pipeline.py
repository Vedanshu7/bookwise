"""
recommendation_pipeline — Airflow DAG.

Compute Apriori association rules from user ratings and write them to the
MongoDB Recommend collection so the Flask /recommend route reads pre-computed
results instead of running ML on every request.

Pipeline stages:
    extract   — copy ratings from MongoDB into a staging collection
    compute   — build basket matrix, run Apriori, write rules to staging
    load      — atomically replace the live Recommend collection
    cleanup   — drop staging collections

Import as:

    import airflow.dags.recommendation_pipeline as adreco
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import airflow
import airflow.operators.python

_LOG = logging.getLogger(__name__)

# =========================================================================
# Configuration.
# =========================================================================

_MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/Bookify")
_DB_NAME = "Bookify"

_COL_RATINGS = "ratings"
_COL_RECOMMEND = "Recommend"
_COL_STAGING_RATINGS = "_staging_ratings"
_COL_STAGING_RULES = "_staging_rules"

_MIN_SUPPORT = float(os.getenv("PIPELINE_MIN_SUPPORT", "0.01"))
_MIN_LIFT = float(os.getenv("PIPELINE_MIN_LIFT", "1.0"))

# =========================================================================
# Task callables.
# =========================================================================


def extract(**context) -> None:
    """
    Copy the ratings collection into a staging collection.

    Push the document count to XCom so downstream tasks can short-circuit
    if no data is available without opening an additional DB connection.

    :param context: Airflow task context passed by the operator
    """
    import pymongo

    client = pymongo.MongoClient(_MONGO_URI)
    db = client[_DB_NAME]
    # Read all ratings from the source collection.
    ratings = list(db[_COL_RATINGS].find({}, {"_id": 0}))
    if not ratings:
        client.close()
        raise ValueError(
            f"Collection '{_COL_RATINGS}' is empty. "
            "Seed the database before running the pipeline."
        )
    # Write ratings to the staging collection for the compute task.
    db[_COL_STAGING_RATINGS].drop()
    db[_COL_STAGING_RATINGS].insert_many(ratings)
    client.close()
    context["ti"].xcom_push(key="rating_count", value=len(ratings))
    _LOG.info("Staged %d ratings.", len(ratings))


def compute(**context) -> None:
    """
    Build a user-book basket matrix, run Apriori, and write rules to staging.

    Expected rating document format::

        {"user": "<user_id>", "book": "<title>", "rating": <1-5>}

    Output rule document format::

        {"antecedents": "<title>", "consequents": "<title>",
         "confidence": <float>, "lift": <float>, "Price_C": <float>}

    :param context: Airflow task context passed by the operator
    """
    import pandas as pd
    import pymongo
    from mlxtend.frequent_patterns import apriori, association_rules

    count = context["ti"].xcom_pull(key="rating_count", task_ids="extract")
    if not count:
        raise ValueError(
            "XCom key 'rating_count' is missing — extract task may have failed."
        )
    client = pymongo.MongoClient(_MONGO_URI)
    db = client[_DB_NAME]
    # Load staged ratings and build one-hot basket matrix.
    ratings = list(db[_COL_STAGING_RATINGS].find({}, {"_id": 0}))
    df = pd.DataFrame(ratings)
    basket = (
        df.groupby(["user", "book"])["rating"]
        .sum()
        .unstack(fill_value=0)
        .map(lambda v: 1 if v > 0 else 0)
        .astype(bool)
    )
    # Compute frequent itemsets and derive association rules.
    frequent_itemsets = apriori(basket, min_support=_MIN_SUPPORT, use_colnames=True)
    if frequent_itemsets.empty:
        client.close()
        raise ValueError(
            f"No frequent itemsets found with min_support={_MIN_SUPPORT}. "
            "Lower PIPELINE_MIN_SUPPORT and retry."
        )
    rules = association_rules(
        frequent_itemsets, metric="lift", min_threshold=_MIN_LIFT
    )
    # Keep only single-antecedent rules to match the template filter.
    rules = rules[rules["antecedents"].apply(len) == 1][
        ["antecedents", "consequents", "lift", "confidence"]
    ]
    rules["antecedents"] = rules["antecedents"].apply(lambda s: next(iter(s)))
    rules["consequents"] = rules["consequents"].apply(lambda s: next(iter(s)))
    # Enrich each rule with the consequent book's price from the catalog.
    prices = {
        b["title"]: b.get("price", 0.0)
        for b in db.books.find({}, {"_id": 0, "title": 1, "price": 1})
    }
    rules["Price_C"] = rules["consequents"].map(lambda t: prices.get(t, 0.0))
    # Write computed rules to the staging collection.
    records = rules.to_dict("records")
    db[_COL_STAGING_RULES].drop()
    db[_COL_STAGING_RULES].insert_many(records)
    client.close()
    context["ti"].xcom_push(key="rule_count", value=len(records))
    _LOG.info("Computed %d association rules.", len(records))


def load(**context) -> None:
    """
    Atomically replace the live Recommend collection with staged rules.

    :param context: Airflow task context passed by the operator
    """
    import pymongo

    rule_count = context["ti"].xcom_pull(key="rule_count", task_ids="compute")
    if not rule_count:
        raise ValueError(
            "XCom key 'rule_count' is missing — compute task may have failed."
        )
    client = pymongo.MongoClient(_MONGO_URI)
    db = client[_DB_NAME]
    # Swap staged rules into the live collection.
    rules = list(db[_COL_STAGING_RULES].find({}, {"_id": 0}))
    db[_COL_RECOMMEND].drop()
    db[_COL_RECOMMEND].insert_many(rules)
    client.close()
    _LOG.info("Loaded %d rules into '%s'.", rule_count, _COL_RECOMMEND)


def cleanup(**_) -> None:
    """
    Drop both staging collections after a successful pipeline run.
    """
    import pymongo

    client = pymongo.MongoClient(_MONGO_URI)
    db = client[_DB_NAME]
    db[_COL_STAGING_RATINGS].drop()
    db[_COL_STAGING_RULES].drop()
    client.close()
    _LOG.info("Staging collections dropped.")


# =========================================================================
# DAG definition.
# =========================================================================

_default_args = {
    "owner": "bookwise",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
}

with airflow.DAG(
    "recommendation_pipeline",
    default_args=_default_args,
    description="Daily Apriori pipeline: ratings → rules → Recommend collection.",
    schedule_interval="@daily",
    catchup=False,
    tags=["bookwise", "recommendations"],
) as dag:

    t_extract = airflow.operators.python.PythonOperator(
        task_id="extract", python_callable=extract
    )
    t_compute = airflow.operators.python.PythonOperator(
        task_id="compute", python_callable=compute
    )
    t_load = airflow.operators.python.PythonOperator(
        task_id="load", python_callable=load
    )
    t_cleanup = airflow.operators.python.PythonOperator(
        task_id="cleanup", python_callable=cleanup
    )
    # Wire the pipeline stages in order.
    t_extract >> t_compute >> t_load >> t_cleanup
