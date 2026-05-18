# bookwise — Improvement Roadmap

## Recommendation Engine

- **Content-based filtering** — use cosine similarity on book genres/tags/descriptions using `scikit-learn`
- **Collaborative filtering** — user-user similarity based on reading history and ratings
- **Hybrid model** — combine content-based and collaborative filtering

## Database — Cassandra Upgrade

MongoDB works well at small scale, but Cassandra's wide-column model is better suited for write-heavy recommendation logs and high-read book catalogue queries.

**Migration plan:**
1. Keep Flask frontend and API layer unchanged
2. Add `cassandra-driver` Python package
3. Schema design:
   - `user_interactions` table — partitioned by `user_id`, clustering on `timestamp`
   - `book_catalog` table — partitioned by `genre`, clustering on `rating`
   - `recommendations` table — partitioned by `user_id`, materialized view by `score`
4. Host on [DataStax Astra](https://astra.datastax.com) free tier
5. Migrate user profiles last (keep MongoDB as fallback during transition)

## Features

- **Search** — full-text search across book titles and authors
- **Ratings** — allow users to rate books (1–5 stars)
- **Reading list** — save books to "want to read" / "currently reading" / "finished"
- **REST API** — expose `/api/recommend` for a React frontend

## Security

- **CSRF protection** — add `Flask-WTF` for form tokens
- **Rate limiting** — prevent brute-force login with `Flask-Limiter`
- **Email verification** — confirm email on registration
