# bookwise

> Discover your next favourite read with personalized book recommendations.

`bookwise` is a web application where users register, log in, and receive book recommendations stored in MongoDB. Built with Flask and a clean Bootstrap-based frontend.

## Features

- User registration and login with **bcrypt password hashing**
- Personalized book recommendation feed
- Book catalogue with individual product pages
- Responsive Bootstrap UI

## Tech Stack

- **Backend:** Python / Flask
- **Database:** MongoDB Atlas
- **Auth:** bcrypt hashing
- **Frontend:** HTML, CSS, JavaScript (Jinja2 templates)

## Setup

### 1. Clone and install

```bash
git clone https://github.com/Vedanshu7/bookwise
cd bookwise
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env`:

```env
MONGO_URI=mongodb+srv://<user>:<pass>@cluster0.mongodb.net/Bookify?retryWrites=true&w=majority
SECRET_KEY=your-random-secret-key
```

### 3. Run

```bash
python src/app.py
```

Open `http://localhost:5000`

## Project Structure

```
bookwise/
├── src/app.py          # Flask routes and app config
├── templates/          # Jinja2 HTML templates
├── static/             # CSS, JS, images
├── requirements.txt
├── .env.example
└── README.md
```

## License

MIT
