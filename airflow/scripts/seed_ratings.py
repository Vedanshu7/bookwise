"""
seed_ratings.py — populate MongoDB with sample ratings for pipeline testing.

Run once after `docker compose up`:
    docker compose exec mongo mongosh Bookify --eval "db.ratings.drop()"
    docker compose exec airflow-worker python /opt/airflow/dags/../scripts/seed_ratings.py

Or directly:
    MONGO_URI=mongodb://localhost:27017/Bookify python airflow/scripts/seed_ratings.py
"""

from __future__ import annotations

import os
import random

import pymongo

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/Bookify")

BOOKS = [
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

random.seed(42)


def generate_ratings(n_users: int = 200) -> list[dict]:
    ratings = []
    for user_idx in range(n_users):
        # Each user rates a random subset of 2-6 books.
        sampled = random.sample(BOOKS, k=random.randint(2, 6))
        for book in sampled:
            ratings.append({
                "user":   f"user_{user_idx:04d}",
                "book":   book["title"],
                "rating": random.randint(3, 5),
            })
    return ratings


def main() -> None:
    client = pymongo.MongoClient(MONGO_URI)
    db = client.Bookify

    db.books.drop()
    db.books.insert_many(BOOKS)
    print(f"Inserted {len(BOOKS)} books into 'books'.")

    db.ratings.drop()
    ratings = generate_ratings()
    db.ratings.insert_many(ratings)
    print(f"Inserted {len(ratings)} ratings into 'ratings'.")

    client.close()


if __name__ == "__main__":
    main()
