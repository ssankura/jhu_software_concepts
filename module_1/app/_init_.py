from flask import Flask

def create_app() -> Flask:
    app = Flask(__name__)

    # Register blueprints
    from app.pages.routes import pages_bp
    app.register_blueprint(pages_bp)

    return app