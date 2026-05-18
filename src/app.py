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

Import as:

    import src.app as srcapp
"""

from __future__ import annotations

import logging
import os
from typing import Callable

import bcrypt
import dotenv
import flask
import flask_pymongo

dotenv.load_dotenv()

_LOG = logging.getLogger(__name__)

# =========================================================================
# App setup.
# =========================================================================

app = flask.Flask(__name__, template_folder="../templates", static_folder="../static")
app.secret_key = os.getenv("SECRET_KEY", "change-me-in-production")

_mongo_uri = os.getenv("MONGO_URI")
if not _mongo_uri:
    raise RuntimeError("MONGO_URI environment variable is required. See .env.example.")

app.config["MONGO_URI"] = _mongo_uri
mongo = flask_pymongo.PyMongo(app)

# =========================================================================
# Auth helpers.
# =========================================================================


def login_required(view: Callable) -> Callable:
    """
    Redirect unauthenticated requests to the login page.

    :param view: the Flask view function to protect
    :return: wrapped view function that checks for an active session
    """
    import functools

    @functools.wraps(view)
    def _wrapped(*args, **kwargs):
        if "username" not in flask.session:
            flask.flash("Please log in to continue.", "warning")
            return flask.redirect(flask.url_for("index"))
        return view(*args, **kwargs)

    return _wrapped


def _hash_password(plain: str) -> bytes:
    """
    Hash a plain-text password with bcrypt.

    :param plain: raw password string provided by the user
    :return: bcrypt hash bytes ready to store in the database
    """
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt())


def _check_password(plain: str, hashed: bytes) -> bool:
    """
    Verify a plain-text password against a stored bcrypt hash.

    :param plain: password provided by the user at login
    :param hashed: bcrypt hash retrieved from the database
    :return: True if the password matches, False otherwise
    """
    return bcrypt.checkpw(plain.encode("utf-8"), hashed)


# =========================================================================
# Routes — public.
# =========================================================================


@app.route("/")
def index() -> str:
    """
    Render the home page.

    :return: rendered HTML for index.html
    """
    return flask.render_template("index.html")


@app.route("/login", methods=["GET", "POST"])
def login() -> str:
    """
    Handle user login.

    GET  — render the login form.
    POST — validate credentials and redirect to home on success.

    :return: rendered HTML or redirect response
    """
    if flask.request.method == "POST":
        username = flask.request.form.get("username", "").strip()
        password = flask.request.form.get("pass", "")
        # Validate that required fields were submitted.
        if not username or not password:
            flask.flash("Username and password are required.", "danger")
            return flask.render_template("login.html")
        # Look up the user and verify credentials.
        user = mongo.db.users.find_one({"name": username})
        if user and _check_password(password, user["password"]):
            flask.session["username"] = username
            _LOG.info("User '%s' logged in.", username)
            return flask.redirect(flask.url_for("index"))
        flask.flash("Invalid username or password.", "danger")
    return flask.render_template("login.html")


@app.route("/register", methods=["GET", "POST"])
def register() -> str:
    """
    Handle new user registration.

    GET  — render the registration form.
    POST — create account if username is free, redirect to home on success.

    :return: rendered HTML or redirect response
    """
    if flask.request.method == "POST":
        username = flask.request.form.get("username", "").strip()
        password = flask.request.form.get("pass", "")
        phone = flask.request.form.get("p_no", "")
        age = flask.request.form.get("age", "")
        # Validate required fields.
        if not username or not password:
            flask.flash("Username and password are required.", "danger")
            return flask.render_template("register.html")
        # Reject duplicate usernames.
        if mongo.db.users.find_one({"name": username}):
            flask.flash("Username already exists. Please choose another.", "warning")
            return flask.render_template("register.html")
        # Create the new user record.
        mongo.db.users.insert_one({
            "name": username,
            "password": _hash_password(password),
            "Phone_No": phone,
            "Age": age,
        })
        flask.session["username"] = username
        flask.flash("Account created! Welcome aboard.", "success")
        _LOG.info("New user '%s' registered.", username)
        return flask.redirect(flask.url_for("index"))
    return flask.render_template("register.html")


@app.route("/logout")
def logout():
    """
    End the user session and redirect to home.

    :return: redirect response to the home page
    """
    _LOG.info("User '%s' logged out.", flask.session.get("username"))
    flask.session.pop("username", None)
    return flask.redirect(flask.url_for("index"))


# =========================================================================
# Routes — catalogue.
# =========================================================================


@app.route("/productsingle")
def productsingle() -> str:
    """
    Render the single-book detail page.

    :return: rendered HTML for product-single.html
    """
    return flask.render_template("product-single.html")


@app.route("/products")
def products() -> str:
    """
    Render the full book catalogue.

    :return: rendered HTML for products.html
    """
    return flask.render_template("products.html")


# =========================================================================
# Routes — recommendations.
# =========================================================================


@app.route("/recommend")
@login_required
def recommend() -> str:
    """
    Render personalised book recommendations for the logged-in user.

    Reads pre-computed association rules from the Recommend collection,
    which is populated by the Airflow recommendation_pipeline DAG.

    :return: rendered HTML for recommend.html with pre-computed rules
    """
    books = list(mongo.db.Recommend.find({}, {"_id": 0}))
    return flask.render_template("recommend.html", x=books)


@app.route("/api/books")
def api_books():
    """
    Return the full book catalogue as JSON.

    Example response::

        [{"title": "...", "genre": "...", "author": "..."}, ...]

    :return: JSON response containing all book documents
    """
    books = list(mongo.db.Recommend.find({}, {"_id": 0}))
    return flask.jsonify(books)


# =========================================================================
# Entry point.
# =========================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    app.run(debug=_debug)
