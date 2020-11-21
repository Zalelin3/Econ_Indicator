# __init__.py 

# third-party imports
from flask import Flask
from flask_bootstrap import Bootstrap
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

def create_app():
    app = Flask(__name__, instance_relative_config=True)
    Bootstrap(app)
    from .main import main
    app.register_blueprint(main)
    # MySQL configurations
    app.config.from_pyfile('config.py')
    db.init_app(app)
    return app