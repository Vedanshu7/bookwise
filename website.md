---
title: Bookwise
type: Data Pipeline
projectURL: bookwise
descriptionShort: A book recommendation app serving results precomputed by an Apache Airflow pipeline — no ML on the request path.
descriptionLong: Bookwise is a Flask web application that serves personalised book recommendations computed offline by an Apache Airflow pipeline. The key design decision is that no ML ever runs on the web request path. A daily Airflow DAG — extract, compute, load, cleanup — runs the Apriori market-basket algorithm across user ratings and writes the resulting association rules into a MongoDB collection. Flask only reads from that collection, so page loads stay fast regardless of how expensive the model gets. Airflow runs on a Celery executor with a Redis broker and PostgreSQL for metadata, while application data (users, books, ratings, and recommendations) lives in MongoDB. The whole stack comes up through Docker Compose.
viewCodeUrl: https://github.com/Vedanshu7/bookwise
viewProjectUrl:
projectImg: /project-image/bookwise.svg
technologies:
  - Python
  - Flask
  - Apache Airflow
  - MongoDB
  - Redis
  - PostgreSQL
  - Docker
---
