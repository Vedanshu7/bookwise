#!/usr/bin/env python
"""
seed_ratings — populate MongoDB with sample books and ratings for pipeline testing.

Run after `docker compose up`:

    docker compose exec airflow-worker \\
        python /opt/airflow/scripts/seed_ratings.py

Or locally:

    MONGO_URI=mongodb://localhost:27017/Bookify python airflow/scripts/seed_ratings.py

Import as:

    import airflow.scripts.seed_ratings as airscseera
"""

from __future__ import annotations

import logging
import os
import random

import pymongo

_LOG = logging.getLogger(__name__)

_MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/Bookify")

_BOOKS = [
    {"title": "The Great Gatsby",       "price": 9.99},
    {"title": "To Kill a Mockingbird",  "price": 12.99},
    {"title": "1984",                   "price": 8.99},
    {"title": "Pride and Prejudice",    "price": 7.99},
    {"title": "The Catcher in the Rye", "price": 10.99},
    {"title": "Brave New World",        "price": 9.49},
    {"title": "The Hobbit",             "price": 14.99},
    {"title": "Harry Potter",           "price": 19.99},
    {"title": "Dune",                   "price": 13.99},
    {"title": "Foundation",             "price": 11.99},
]

# Fix the seed so generated ratings are reproducible across runs.
random.seed(42)


def _generate_ratings(n_users: int = 200) -> list[dict]:
    """
    Generate synthetic per-user book ratings.

    Each user rates a random subset of 2–6 books with a rating in [3, 5].

    :param n_users: number of synthetic users to generate
    :return: list of rating documents with keys: user, book, rating
    """
    ratings = []
    for user_idx in range(n_users):
        # Sample a random subset of books for this user.
        sampled = random.sample(_BOOKS, k=random.randint(2, 6))
        for book in sampled:
            ratings.append({
                "user":   f"user_{user_idx:04d}",
                "book":   book["title"],
                "rating": random.randint(3, 5),
            })
    return ratings


def main() -> None:
    """
    Seed the books and ratings collections in MongoDB.
    """
    client = pymongo.MongoClient(_MONGO_URI)
    db = client.Bookify
    # Populate the book catalog.
    db.books.drop()
    db.books.insert_many(_BOOKS)
    _LOG.info("Inserted %d books into 'books'.", len(_BOOKS))
    # Populate the ratings used by the pipeline.
    db.ratings.drop()
    ratings = _generate_ratings()
    db.ratings.insert_many(ratings)
    _LOG.info("Inserted %d ratings into 'ratings'.", len(ratings))
    client.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
