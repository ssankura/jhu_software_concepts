"""
app/__init__.py

This module creates and configures the Flask application using the
Application Factory pattern.

Responsibilities:
----------------
• Initialize Flask app
• Register Blueprints
• Prepare application configuration

Using an application factory allows modular and scalable Flask design.
"""

"""
app/__init__.py

Creates and configures the Flask application using the Application Factory pattern.
Registers Blueprints and sets template/static folder locations.
"""

from flask import Flask
from pathlib import Path


def create_app() -> Flask:
    """
    Application factory function.

    Returns:
        Flask: Configured Flask application object
    """
    base_dir = Path(__file__).resolve().parent.parent  # module_3/

    app = Flask(
        __name__,
        template_folder=str(base_dir / "templates"),
        static_folder=str(base_dir / "static"),
    )
    app.secret_key = "jhu_module3_secret_key"


    # Register Blueprints
    from app.pages import pages_bp
    app.register_blueprint(pages_bp)

    return app
