import json
import requests
from bs4 import BeautifulSoup
import feedgen.feed
import logging
import logging.handlers
from typing import Dict, List, Set
import time
import random
import os
import psycopg2
import psycopg2.extras
import PyPDF2
import io
from datetime import datetime
from dotenv import load_dotenv


class WebRSSCrawler:
    def __init__(
        self,
        config_file: str,
        log_level: int = logging.INFO,
        log_file: str = 'logs/web_rss_crawler.log',
        rss_directory: str = 'rss'
    ):
        # Load environment variables from .env file
        load_dotenv()
        self.logger = self._setup_logger(log_level, log_file)

        # Ensure that /logs and /rss directories exist
        self._ensure_directory_exists('logs')
        self._ensure_directory_exists(rss_directory)

        self.rss_directory = rss_directory

        self.logger.debug(
            f"Attempting to load configuration from {config_file}")
        try:
            with open(config_file, 'r') as f:
                self.configs = json.load(f)
            self.logger.debug(
                f"Successfully loaded configuration: {self.configs}")
        except Exception as e:
            self.logger.error(f"Error loading config file: {e}")
            raise

        self._initialize_db()

        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        ]

    def _ensure_directory_exists(self, directory: str):
        """Ensure that a directory exists; if not, create it."""
        try:
            os.makedirs(directory, exist_ok=True)
            self.logger.debug(f"Ensured existence of directory: {directory}")
        except Exception as e:
            self.logger.error(f"Failed to create directory {directory}: {e}")
            raise

    def _initialize_db(self):
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()

            # Create all_links table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS all_links (
                    id SERIAL PRIMARY KEY,
                    feed_title TEXT NOT NULL,
                    link TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    is_pdf BOOLEAN NOT NULL DEFAULT FALSE,
                    content_type TEXT,
                    last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    times_seen INTEGER DEFAULT 1,
                    http_status INTEGER,
                    UNIQUE(feed_title, link)
                )
            """)

            # Create pdf_content table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pdf_content (
                    id SERIAL PRIMARY KEY,
                    feed_title TEXT NOT NULL,
                    source_link TEXT NOT NULL,
                    pdf_url TEXT NOT NULL,
                    content TEXT,
                    title TEXT,
                    page_title TEXT,
                    author TEXT,
                    creation_date TEXT,
                    modification_date TEXT,
                    number_of_pages INTEGER,
                    file_size_bytes INTEGER,
                    date_processed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (feed_title, pdf_url) REFERENCES all_links(feed_title, link),
                    UNIQUE(feed_title, pdf_url)
                )
            """)

            # Create seen_links table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS seen_links (
                    feed_title TEXT NOT NULL,
                    link TEXT NOT NULL,
                    PRIMARY KEY (feed_title, link)
                )
            """)

            conn.commit()
            cursor.close()
            conn.close()
            self.logger.debug(
                "Initialized PostgreSQL database with all necessary tables")
        except psycopg2.Error as e:
            self.logger.error(f"Failed to initialize PostgreSQL database: {e}")
            raise

    def _get_db_connection(self):
        """Establishes a connection to the PostgreSQL database using credentials from environment variables."""
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
            self.logger.error(f"Error connecting to PostgreSQL: {e}")
            raise

    def _extract_pdf_metadata(self, pdf_content: bytes) -> Dict:
        """Extract metadata from PDF content"""
        try:
            pdf_file = io.BytesIO(pdf_content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)

            # Extract text content
            text_content = ""
            for page in pdf_reader.pages:
                page_text = page.extract_text()
                text_content += page_text if page_text else ""

            # Get metadata
            metadata = pdf_reader.metadata if pdf_reader.metadata else {}

            return {
                'content': text_content,
                'title': metadata.get('/Title', ''),
                'author': metadata.get('/Author', ''),
                'creation_date': metadata.get('/CreationDate', ''),
                'modification_date': metadata.get('/ModDate', ''),
                'number_of_pages': len(pdf_reader.pages),
                'file_size_bytes': len(pdf_content)
            }
        except Exception as e:
            self.logger.error(f"Error extracting PDF metadata: {e}")
            return {}

    def _setup_logger(self, log_level: int, log_file: str) -> logging.Logger:
        """Sets up the logger to log to both console and file."""
        logger = logging.getLogger(__name__)
        logger.setLevel(log_level)

        if logger.hasHandlers():
            logger.handlers.clear()

        # Console Handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        # File Handler
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=1_000_000, backupCount=5
        )
        file_handler.setLevel(log_level)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        return logger

    def _get_random_headers(self) -> Dict[str, str]:
        """Generates random headers to mimic different browsers."""
        chosen_user_agent = random.choice(self.user_agents)
        self.logger.debug(f"Chosen user agent: {chosen_user_agent}")
        headers = {
            'User-Agent': chosen_user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        self.logger.debug(f"Generated request headers: {headers}")
        return headers

    def _safe_request(self, url: str, timeout: int = 10) -> requests.Response:
        """Performs a safe HTTP GET request with error handling."""
        self.logger.debug(
            f"Starting safe request to {url} with timeout={timeout}")
        try:
            delay = random.uniform(0, 0.05)
            self.logger.debug(
                f"Sleeping for random delay: {delay:.2f} seconds before request")
            time.sleep(delay)

            response = requests.get(
                url, headers=self._get_random_headers(), timeout=timeout, stream=True
            )
            response.raise_for_status()
            self.logger.debug(
                f"Received response (status: {response.status_code}) for {url}")
            return response
        except requests.RequestException as e:
            self.logger.error(f"Request failed for {url}: {e}")
            return None

    def _extract_links(self, soup: BeautifulSoup, link_selector: str) -> List[str]:
        """Extracts links from the BeautifulSoup object based on the provided CSS selector."""
        self.logger.debug(f"Extracting links using selector '{link_selector}'")
        try:
            links = soup.select(link_selector)
            extracted = []
            for link in links:
                href = link.get('href')
                src = link.get('src')
                if href:
                    extracted.append(href)
                elif src:
                    extracted.append(src)
            self.logger.debug(
                f"Extracted {len(extracted)} links from selector '{link_selector}'")
            return extracted
        except Exception as e:
            self.logger.error(f"Error extracting links: {e}")
            return []

    def _extract_all_existing_links(self, conn, feed_title: str) -> Set[str]:
        """Fetches all existing links for a specific feed and returns them as a set."""
        self.logger.debug(
            f"Loading all existing links for feed '{feed_title}' into memory.")
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT link FROM all_links WHERE feed_title = %s",
                (feed_title,)
            )
            rows = cursor.fetchall()
            existing_links = {row[0] for row in rows}
            cursor.close()
            self.logger.debug(
                f"Loaded {len(existing_links)} existing links for feed '{feed_title}'.")
            return existing_links
        except psycopg2.Error as e:
            self.logger.error(
                f"Failed to load existing links for feed '{feed_title}': {e}")
            return set()

    def _batch_insert_new_links(self, conn, feed_title: str, new_links: List[Dict]):
        """Inserts new links into the all_links table in a single batch operation."""
        if not new_links:
            return
        try:
            cursor = conn.cursor()
            insert_query = """
                INSERT INTO all_links (
                    feed_title, link, source_url, is_pdf, content_type
                ) VALUES %s
                ON CONFLICT (feed_title, link) DO NOTHING
            """
            values = [
                (
                    feed_title,
                    link['link'],
                    link['source_url'],
                    link['is_pdf'],
                    link['content_type']
                )
                for link in new_links
            ]
            psycopg2.extras.execute_values(
                cursor, insert_query, values, template=None, page_size=100
            )
            conn.commit()
            cursor.close()
            self.logger.debug(
                f"Inserted {len(new_links)} new links for feed '{feed_title}'.")
        except psycopg2.Error as e:
            self.logger.error(
                f"Failed to batch insert new links for feed '{feed_title}': {e}")

    def _process_pdf_batch(self, conn, pdf_links: List[Dict]):
        """Processes a batch of PDF links."""
        for pdf_link in pdf_links:
            pdf_url = pdf_link['link']
            feed_title = pdf_link['feed_title']
            source_url = pdf_link['source_url']
            self._process_pdf(conn, pdf_url, feed_title, source_url)

    def _process_pdf(self, conn, pdf_url: str, feed_title: str, source_link: str) -> bool:
        """Processes a PDF file and stores its content and metadata in the database."""
        self.logger.debug(
            f"Starting to process PDF: {pdf_url} for feed: {feed_title}")
        try:
            # Check if PDF already processed
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id FROM pdf_content WHERE feed_title = %s AND pdf_url = %s",
                (feed_title, pdf_url)
            )
            if cursor.fetchone():
                self.logger.debug(f"PDF {pdf_url} already processed.")
                cursor.close()
                return False

            # Verify that this PDF link exists in all_links
            cursor.execute(
                "SELECT id FROM all_links WHERE feed_title = %s AND link = %s",
                (feed_title, pdf_url)
            )
            if not cursor.fetchone():
                self.logger.debug(
                    f"PDF link {pdf_url} not found in all_links. Cannot process.")
                cursor.close()
                return False

            cursor.close()

            # Download PDF content
            response = self._safe_request(pdf_url)
            if not response or response.status_code != 200:
                self.logger.error(f"Failed to download PDF {pdf_url}")
                return False

            # Extract metadata and content
            metadata = self._extract_pdf_metadata(response.content)

            # Create a URL-friendly page title from the Feed title and date
            current_date = datetime.now().strftime('%Y-%m-%d')
            page_title = f"{feed_title.replace(' ', '_')}_{current_date}"
            metadata['page_title'] = page_title

            # Insert PDF metadata
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO pdf_content (
                    feed_title, source_link, pdf_url, content, title, page_title, author,
                    creation_date, modification_date, number_of_pages, file_size_bytes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                feed_title,
                source_link,
                pdf_url,
                metadata.get('content', ''),
                metadata.get('title', ''),
                metadata.get('page_title', ''),
                metadata.get('author', ''),
                metadata.get('creation_date', ''),
                metadata.get('modification_date', ''),
                metadata.get('number_of_pages', 0),
                metadata.get('file_size_bytes', 0)
            ))

            conn.commit()
            cursor.close()

            content_preview = metadata.get('content', '')[
                :50] + '...' if metadata.get('content') else 'No content'
            self.logger.info(f"Stored PDF content for {pdf_url}")
            self.logger.info(f"Content preview: {content_preview}")
            return True

        except psycopg2.Error as e:
            self.logger.error(f"Database error processing PDF {pdf_url}: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error processing PDF {pdf_url}: {e}")
            return False

    def generate_rss_feeds(self):
        """Generates RSS feeds based on the configurations provided."""
        self.logger.info(
            "Starting RSS feed generation for all configurations.")

        try:
            # Establish a single database connection for the entire process
            conn = self._get_db_connection()
        except Exception as e:
            self.logger.error(f"Failed to establish database connection: {e}")
            return

        for i, config in enumerate(self.configs, start=1):
            self.logger.info(
                f"Processing config {i}/{len(self.configs)}: {config.get('feed_title', 'Unnamed Feed')}"
            )

            feed_title = config.get('feed_title', 'Web Crawler Feed')
            output_filename = config.get(
                'output_filename', f"{feed_title.replace(' ', '_')}_feed.xml"
            )
            source_url = config.get("source_url", "")
            pdf_only = config.get('pdf_only', False)

            # Ensure RSS directory is used
            output_path = os.path.join(self.rss_directory, output_filename)

            try:
                feed_gen = feedgen.feed.FeedGenerator()
                feed_gen.title(feed_title)
                feed_gen.description(config.get(
                    'feed_description', 'Automatically generated feed'
                ))
                feed_gen.link(href=source_url)

                response = self._safe_request(source_url)
                if not response:
                    self.logger.warning(
                        f"Skipping feed '{feed_title}' due to failed request."
                    )
                    continue

                soup = BeautifulSoup(response.content, 'html.parser')
                links = self._extract_links(
                    soup, config.get('link_selector', 'a')
                )

                # Load all existing links for this feed into a set
                existing_links = self._extract_all_existing_links(
                    conn, feed_title)

                new_links = []
                new_pdf_links = []

                for link in links:
                    full_link = requests.compat.urljoin(source_url, link) if not link.startswith(
                        ('http://', 'https://')) else link

                    if full_link in existing_links:
                        continue  # Skip processing since it's not new

                    # Determine if the link is a PDF
                    is_pdf = self._is_pdf_link(full_link)
                    content_type = 'application/pdf' if is_pdf else 'unknown'

                    # Add to new_links for batch insertion
                    new_links.append({
                        'link': full_link,
                        'feed_title': feed_title,
                        'source_url': source_url,
                        'is_pdf': is_pdf,
                        'content_type': content_type
                    })

                    # If it's a new PDF, add to the PDF processing list
                    if is_pdf:
                        new_pdf_links.append({
                            'link': full_link,
                            'feed_title': feed_title,
                            'source_url': source_url
                        })

                # Batch insert new links
                self._batch_insert_new_links(conn, feed_title, new_links)

                # Batch process PDFs
                self._process_pdf_batch(conn, new_pdf_links)

                # Add new links to RSS feed
                for link_entry in new_links:
                    link = link_entry['link']
                    is_pdf = link_entry['is_pdf']
                    if pdf_only and not is_pdf:
                        continue  # Skip non-PDF links in PDF-only mode

                    fe = feed_gen.add_entry()
                    # Use a meaningful title; here using the last part of the URL or a default
                    entry_title = link.split('/')[-1] if '/' in link else link
                    fe.title(entry_title)
                    fe.link(href=link)

                # Save feed to the rss directory
                feed_gen.rss_file(output_path)
                self.logger.info(
                    f"Saved RSS feed for '{feed_title}' to '{output_path}'"
                )

            except Exception as e:
                self.logger.error(f"Error processing config {config}: {e}")

        # Close the database connection after processing all feeds
        try:
            conn.close()
            self.logger.debug("Closed the database connection.")
        except Exception as e:
            self.logger.error(f"Error closing database connection: {e}")

        self.logger.info(
            "Completed RSS feed generation for all configurations.")

    def _is_pdf_link(self, url: str) -> bool:
        """
        Check if the given URL points to a PDF by using a GET request and inspecting the 'Content-Type' header.
        """
        self.logger.debug(f"Checking if URL is a PDF: {url}")
        try:
            headers = self._get_random_headers()
            # Using GET instead of HEAD to handle servers that don't respond properly to HEAD
            response = requests.get(
                url, headers=headers, allow_redirects=True, timeout=10, stream=True
            )
            response.raise_for_status()
            content_type = response.headers.get('Content-Type', '').lower()
            self.logger.debug(f"URL: {url} | Content-Type: {content_type}")
            # Close the response stream since we only needed headers
            response.close()
            return 'application/pdf' in content_type
        except requests.RequestException as e:
            self.logger.error(f"Failed to verify Content-Type for {url}: {e}")
            return False


def run_scraper():
    """
    Initializes and runs the WebRSSCrawler to generate RSS feeds.
    """
    try:
        crawler = WebRSSCrawler(
            config_file='crawler_config.json',
            log_level=logging.DEBUG,
            rss_directory='rss'  # Ensure RSS feeds are saved to the 'rss' folder
        )
        crawler.generate_rss_feeds()
    except Exception as e:
        logger = logging.getLogger('WebRSSCrawler')
        logger.error(f"Failed to run scraper: {e}")


