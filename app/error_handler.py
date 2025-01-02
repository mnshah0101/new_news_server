from functools import wraps
from flask import jsonify
import psycopg2
import logging
from typing import Callable, Any


class APIErrorHandler:
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def handle_endpoint(self, f: Callable) -> Callable:
        @wraps(f)
        def decorated_function(*args: Any, **kwargs: Any):
            try:
                return f(*args, **kwargs)
            except psycopg2.Error as e:
                self.logger.error(f"Database error in {f.__name__}: {str(e)}")
                return jsonify({
                    'error': 'Database error occurred',
                    'message': str(e),
                    'type': 'database_error'
                }), 500
            except ValueError as e:
                self.logger.error(
                    f"Validation error in {f.__name__}: {str(e)}")
                return jsonify({
                    'error': 'Validation error',
                    'message': str(e),
                    'type': 'validation_error'
                }), 400
            except FileNotFoundError as e:
                self.logger.error(f"File not found in {f.__name__}: {str(e)}")
                return jsonify({
                    'error': 'Resource not found',
                    'message': str(e),
                    'type': 'not_found_error'
                }), 404
            except Exception as e:
                self.logger.error(
                    f"Unexpected error in {f.__name__}: {str(e)}")
                return jsonify({
                    'error': 'An unexpected error occurred',
                    'message': str(e),
                    'type': 'internal_error'
                }), 500
        return decorated_function
