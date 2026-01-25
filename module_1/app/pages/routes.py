from flask import Blueprint, render_template

pages_bp = Blueprint("pages", __name__)

@pages_bp.get("/")
def home():
    return render_template("home.html", active="home")

@pages_bp.get("/contact")
def contact():
    return render_template("contact.html", active="contact")

@pages_bp.get("/projects")
def projects():
    return render_template("projects.html", active="projects")
