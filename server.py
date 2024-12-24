from flask import Flask, jsonify, request, send_from_directory, abort
import psycopg2
import psycopg2.extras
from datetime import datetime
from flask_cors import CORS
import os
from dotenv import load_dotenv
import logging
import logging.handlers
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from app.scraper import run_scraper

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)
CORS(app)

# Set up logging


def setup_logger():
    logger = logging.getLogger('WebRSSCrawlerAPI')
    logger.setLevel(logging.DEBUG)

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File Handler
    log_directory = 'logs'
    os.makedirs(log_directory, exist_ok=True)
    file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(log_directory, 'web_rss_crawler_api.log'),
        maxBytes=1_000_000,
        backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    return logger


logger = setup_logger()




def get_db_connection():
    """
    Establishes a connection to the PostgreSQL database using credentials from environment variables.
    Returns a connection object.
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT', 5432),
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            sslmode='require'  # Ensure SSL is used
        )
        return conn
    except psycopg2.Error as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        raise


@app.route('/api/articles/date_range', methods=['GET'])
def get_by_last_date_range():
    """
    Get PDF articles by date_processed range from the pdf_content table

    Query parameters:
    - start: Start date in YYYY-MM-DD format (required)
    - end: End date in YYYY-MM-DD format (required)
    """
    start_date = request.args.get('start', '').strip()
    end_date = request.args.get('end', '').strip()

    logger.debug(f"Start Date: {start_date}")
    logger.debug(f"End Date: {end_date}")

    if not start_date or not end_date:
        return jsonify({'error': 'start and end parameters are required'}), 400

    # Validate date format
    try:
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD.'}), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        query = """
            SELECT 
                id,
                feed_title,
                source_link,
                pdf_url,
                content,
                title,
                page_title,
                author,
                creation_date,
                modification_date,
                number_of_pages,
                file_size_bytes,
                date_processed
            FROM pdf_content
            WHERE DATE(date_processed) BETWEEN DATE(%s) AND DATE(%s)
            ORDER BY date_processed DESC
        """

        cursor.execute(query, (start_date, end_date))
        rows = cursor.fetchall()

        articles = [dict(row) for row in rows]

        cursor.close()
        conn.close()
        return jsonify({'articles': articles})

    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/articles', methods=['GET'])
def get_all_articles_by_source_url():
    """
    Get all PDF articles filtered by source_url from the pdf_content table

    Query parameters:
    - source_url: The source URL to filter articles by (required)
    """
    source_url = request.args.get('source_url', '').strip()
    if not source_url:
        return jsonify({'error': 'source_url parameter is required'}), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        query = """
            SELECT 
                id,
                feed_title,
                source_link,
                pdf_url,
                content,
                title,
                page_title,
                author,
                creation_date,
                modification_date,
                number_of_pages,
                file_size_bytes,
                date_processed
            FROM pdf_content
            WHERE source_link = %s
            ORDER BY date_processed DESC
        """

        cursor.execute(query, (source_url,))
        rows = cursor.fetchall()

        articles = [dict(row) for row in rows]

        cursor.close()
        conn.close()
        return jsonify({'articles': articles})

    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/article', methods=['GET'])
def get_article_by_id():
    """
    Get a specific PDF article by id from the pdf_content table.

    Query parameters:
    - id: The unique identifier of the article (required)
    """
    article_id = request.args.get('id', '').strip()
    if not article_id:
        return jsonify({'error': 'id parameter is required'}), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        query = """
            SELECT 
                id,
                feed_title,
                source_link,
                pdf_url,
                content,
                title,
                page_title,
                author,
                creation_date,
                modification_date,
                number_of_pages,
                file_size_bytes,
                date_processed
            FROM pdf_content
            WHERE id = %s
            LIMIT 1
        """

        cursor.execute(query, (article_id,))
        row = cursor.fetchone()

        if not row:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Article not found'}), 404

        article = dict(row)

        cursor.close()
        conn.close()
        return jsonify(article)

    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/articles/all', methods=['GET'])
def get_all_articles():
    """
    Get all PDF articles from the pdf_content table
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        query = """
            SELECT 
                id,
                feed_title,
                source_link,
                pdf_url,
                content,
                title,
                page_title,
                author,
                creation_date,
                modification_date,
                number_of_pages,
                file_size_bytes,
                date_processed
            FROM pdf_content
            ORDER BY date_processed DESC
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        articles = [dict(row) for row in rows]

        cursor.close()
        conn.close()
        return jsonify({'articles': articles})

    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({'error': str(e)}), 500

# New Routes to Serve RSS Feeds


@app.route('/rss/<path:filename>', methods=['GET'])
def serve_rss_feed(filename):
    """
    Serves the specified RSS feed XML file from the rss directory.

    Example:
    GET /rss/example_feed.xml
    """
    rss_directory = os.path.join(os.getcwd(), 'rss')

    # Security: Ensure that only .xml files are served
    if not filename.lower().endswith('.xml'):
        logger.warning(f"Attempt to access non-XML file: {filename}")
        abort(404)

    # Prevent directory traversal attacks
    if '..' in filename or filename.startswith('/'):
        logger.warning(
            f"Attempted directory traversal with filename: {filename}")
        abort(404)

    try:
        return send_from_directory(
            directory=rss_directory,
            filename=filename,
            mimetype='application/rss+xml',
            as_attachment=False
        )
    except FileNotFoundError:
        logger.error(f"RSS feed file not found: {filename}")
        abort(404)


@app.route('/rss', methods=['GET'])
def list_rss_feeds():
    """
    Lists all available RSS feeds in the rss directory.
    
    Example:
    GET /rss
    Response:
    {
        "feeds": [
            "example_feed.xml",
            "another_feed.xml"
        ]
    }
    """
    rss_directory = os.path.join(os.getcwd(), 'rss')
    try:
        files = os.listdir(rss_directory)
        xml_files = [f for f in files if f.lower().endswith('.xml')]
        return jsonify({'feeds': xml_files})
    except Exception as e:
        logger.error(f"Error listing RSS feeds: {e}")
        return jsonify({'error': 'Unable to list RSS feeds'}), 500

# Optional: Root Route for Basic Information


@app.route('/', methods=['GET'])
def index():
    """
    Provides basic information about the API.
    """
    logger.info("Welcome to the WebRSSCrawler API!")
    return jsonify({
        'message': 'Welcome to the WebRSSCrawler API!',
        'endpoints': {
            '/api/articles/date_range': 'Get articles by date range',
            '/api/articles': 'Get articles by source URL',
            '/api/article': 'Get article by ID',
            '/api/articles/all': 'Get all articles',
            '/rss/<filename>': 'Access specific RSS feed',
            '/rss': 'List all RSS feeds'
        }
    })








def schedule_scraper():
    """
    Schedules the scraper to run every 24 hours.
    """
    scheduler = BackgroundScheduler()
    scheduler.start()
    

    # Schedule run_scraper to be called every 24 hours
    scheduler.add_job(
        func=run_scraper,
        trigger=IntervalTrigger(hours=24),
        id='run_scraper_job',
        name='Run scraper every 24 hours',
        replace_existing=True
    )
    logger.info("Scheduled scraper to run every 24 hours.")

    # Shut down the scheduler when exiting the app
    import atexit
    atexit.register(lambda: scheduler.shutdown())


def initialize():
    """
    Initializes the scheduler before the first request is processed.
    """
    print("Initializing scheduler...")
    logger.info("Initializing scheduler...")
    schedule_scraper()


# New Routes to Serve RSS Feeds already included above


if __name__ == '__main__':
    # Ensure that the rss directory exists
    run_scraper()
    rss_dir = 'rss'
    if not os.path.exists(rss_dir):
        os.makedirs(rss_dir)
        logger.debug(f"Created RSS directory at {rss_dir}")
    
    initialize()
    app.run(debug=True, port=5000)
