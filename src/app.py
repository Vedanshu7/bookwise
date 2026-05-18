"""
bookwise — Flask book recommendation web application.

Routes:
    GET  /              Home page
    GET  /login         Login form
    POST /login         Authenticate user
    GET  /register      Registration form
    POST /register      Create new user account
    GET  /logout        End session
    GET  /recommend     Personalised book recommendations
    GET  /products      Book catalogue
    GET  /productsingle Single book page
    GET  /api/books     JSON list of all books (for API consumers)
"""

from __future__ import annotations

import functools
import os
from typing import Callable

import bcrypt
from dotenv import load_dotenv
from flask import (
    Flask,
    flash,
    redirect,
    render_template,
    request,
    session,
    url_for,
    jsonify,
)
from flask_pymongo import PyMongo

load_dotenv()

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = Flask(__name__, template_folder="../templates", static_folder="../static")
app.secret_key = os.getenv("SECRET_KEY", "change-me-in-production")

mongo_uri = os.getenv("MONGO_URI")
if not mongo_uri:
    raise RuntimeError("MONGO_URI environment variable is required. See .env.example.")

app.config["MONGO_URI"] = mongo_uri
mongo = PyMongo(app)

# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------


def login_required(view: Callable) -> Callable:
    """Decorator that redirects unauthenticated requests to the login page."""

    @functools.wraps(view)
    def wrapped(*args, **kwargs):
        if "username" not in session:
            flash("Please log in to continue.", "warning")
            return redirect(url_for("index"))
        return view(*args, **kwargs)

    return wrapped


def _hash_password(plain: str) -> bytes:
    """Hash a plain-text password with bcrypt.

    Args:
        plain: The raw password string.

    Returns:
        bcrypt hash bytes ready to store in the database.
    """
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt())


def _check_password(plain: str, hashed: bytes) -> bool:
    """Verify a plain-text password against a stored bcrypt hash.

    Args:
        plain:   The password provided by the user.
        hashed:  The bcrypt hash retrieved from the database.

    Returns:
        True if the password matches, False otherwise.
    """
    return bcrypt.checkpw(plain.encode("utf-8"), hashed)


# ---------------------------------------------------------------------------
# Routes — public
# ---------------------------------------------------------------------------


@app.route("/")
def index():
    """Render the home page."""
    return render_template("index.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    """Handle user login.

    GET  — render the login form.
    POST — validate credentials; redirect to home on success or show error.
    """
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("pass", "")

        if not username or not password:
            flash("Username and password are required.", "danger")
            return render_template("login.html")

        user = mongo.db.users.find_one({"name": username})
        if user and _check_password(password, user["password"]):
            session["username"] = username
            return redirect(url_for("index"))

        flash("Invalid username or password.", "danger")

    return render_template("login.html")


@app.route("/register", methods=["GET", "POST"])
def register():
    """Handle new user registration.

    GET  — render the registration form.
    POST — create account if username is free, redirect to home on success.
    """
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("pass", "")
        phone = request.form.get("p_no", "")
        age = request.form.get("age", "")

        if not username or not password:
            flash("Username and password are required.", "danger")
            return render_template("register.html")

        if mongo.db.users.find_one({"name": username}):
            flash("Username already exists. Please choose another.", "warning")
            return render_template("register.html")

        mongo.db.users.insert_one({
            "name": username,
            "password": _hash_password(password),
            "Phone_No": phone,
            "Age": age,
        })
        session["username"] = username
        flash("Account created! Welcome aboard.", "success")
        return redirect(url_for("index"))

    return render_template("register.html")


@app.route("/logout")
def logout():
    """End the user session and redirect to home."""
    session.pop("username", None)
    return redirect(url_for("index"))


# ---------------------------------------------------------------------------
# Routes — catalogue
# ---------------------------------------------------------------------------


@app.route("/productsingle")
def productsingle():
    """Render the single-book detail page."""
    return render_template("product-single.html")


@app.route("/products")
def products():
    """Render the full book catalogue."""
    return render_template("products.html")


# ---------------------------------------------------------------------------
# Routes — recommendations
# ---------------------------------------------------------------------------


@app.route("/recommend")
@login_required
def recommend():
    """Render personalised book recommendations for the logged-in user.

    Fetches the full Recommend collection and passes it to the template.
    For a production upgrade, replace this with the content-based filtering
    logic described in IMPROVEMENTS.md.
    """
    books = list(mongo.db.Recommend.find({}, {"_id": 0}))
    return render_template("recommend.html", x=books)


@app.route("/api/books")
def api_books():
    """Return the full book catalogue as JSON.

    Example response::

        [{"title": "...", "genre": "...", "author": "..."}, ...]

    This endpoint enables a future React frontend or mobile app to consume
    the catalogue without server-side rendering.
    """
    books = list(mongo.db.Recommend.find({}, {"_id": 0}))
    return jsonify(books)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    app.run(debug=debug)
