from flask import Flask


def init_app(app: Flask) -> None:
    """Initialize the ManGO Flow application with the given Flask app instance."""
    # Import and register blueprints, extensions, or any other initialization code here
    from .flask.report import mango_flow_report_bp

    app.register_blueprint(mango_flow_report_bp)