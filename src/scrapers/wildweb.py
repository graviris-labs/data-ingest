# src/scrapers/wildweb_scraper.py

import requests
from bs4 import BeautifulSoup
import logging
import sqlite3
import os
import json
from datetime import datetime
import argparse
import time
import re
from pathlib import Path
import uuid
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/wildweb_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class WildWebScraper:
    """
    Scraper for wildfire data from WildWeb dispatch centers.
    First retrieves dispatch centers and their states, then for each center retrieves wildfire data.
    """
    
    def __init__(self, db_path):
        """
        Initialize the scraper with database path.
        
        Args:
            db_path (str): Path to SQLite database
        """
        self.base_url = "http://www.wildcad.net/WildCADWeb.asp"
        self.db_path = db_path
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml',
            'Accept-Language': 'en-US,en;q=0.9'
        })
        self._ensure_db_exists()
        self.api_endpoints = {}
        self._load_saved_api_endpoints()

    def _load_saved_api_endpoints(self):
        """Load saved API endpoints from database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create table if not exists
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS api_endpoints (
                center_code TEXT PRIMARY KEY,
                endpoint_url TEXT,
                last_successful TIMESTAMP
            )
            ''')
            
            # Load existing endpoints
            cursor.execute("SELECT center_code, endpoint_url FROM api_endpoints")
            for row in cursor.fetchall():
                self.api_endpoints[row[0]] = row[1]
                
            conn.close()
            if self.api_endpoints:
                logger.info(f"Loaded {len(self.api_endpoints)} saved API endpoints")
        except Exception as e:
            logger.error(f"Error loading saved API endpoints: {str(e)}")

    def _save_api_endpoint(self, center_code, endpoint_url):
        """Save a successful API endpoint to database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS api_endpoints (
                center_code TEXT PRIMARY KEY,
                endpoint_url TEXT,
                last_successful TIMESTAMP
            )
            ''')
            
            cursor.execute('''
            INSERT OR REPLACE INTO api_endpoints (center_code, endpoint_url, last_successful)
            VALUES (?, ?, ?)
            ''', (center_code, endpoint_url, datetime.now().isoformat()))
            
            conn.commit()
            conn.close()
            
            # Update in-memory cache
            self.api_endpoints[center_code] = endpoint_url
        except Exception as e:
            logger.error(f"Error saving API endpoint: {str(e)}")
            
    def _ensure_db_exists(self):
        """Ensure database and tables exist"""
        db_dir = os.path.dirname(self.db_path)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create dispatch_centers table with UUID as primary key
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS dispatch_centers (
            id TEXT PRIMARY KEY,  -- UUID as text
            center_code TEXT UNIQUE,
            center_name TEXT,
            state TEXT,
            status TEXT,
            url TEXT,
            last_updated TIMESTAMP
        )
        ''')
        
        # Create incidents table with UUID as primary key and foreign key
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS incidents (
            id TEXT PRIMARY KEY,  -- UUID as text (always new for tracking status changes)
            center_id TEXT,       -- UUID reference to dispatch_centers
            incident_id TEXT UNIQUE,     -- Deterministic UUID to identify the same incident
            incident_number INTEGER,
            fire_number INTEGER,
            incident_uuid TEXT,
            fiscal TEXT,
            wfdssunit TEXT,
            incident_command TEXT,
            incident_name TEXT,
            incident_type TEXT,
            incident_status TEXT,
            local_date TIMESTAMP,
            location TEXT,
            latitude REAL,
            longitude REAL,
            resources TEXT,
            acres REAL,
            fuels TEXT,
            comments REAL,
            raw_data TEXT,
            ingest_date TIMESTAMP,
            FOREIGN KEY (center_id) REFERENCES dispatch_centers(id)
        )
        ''')
        
        # Create index on incident_id for efficient querying
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_incident_id ON incidents(incident_id)
        ''')
        
        conn.commit()
        conn.close()
    
    def _generate_center_uuid(self, center_code):
        """
        Generate a deterministic UUID for a dispatch center based on its code.
        This ensures the same center always gets the same UUID.
        
        Args:
            center_code (str): Unique center code (e.g., 'CAANCC')
            
        Returns:
            str: UUID string
        """
        # Use UUID5 with the DNS namespace for deterministic generation
        # This way, the same center_code will always produce the same UUID
        namespace = uuid.NAMESPACE_DNS
        return str(uuid.uuid5(namespace, f"wildweb.dispatch.center.{center_code}"))
    
    def _generate_deterministic_incident_uuid(self, center_code, incident_number, incident_name, incident_status):
        """
        Generate a deterministic UUID for identifying the same incident across scrapes.
        
        Args:
            center_code (str): Center code (e.g., 'CAANCC')
            incident_number (str): Incident number
            incident_name (str): Incident name
            
        Returns:
            str: UUID string
        """
        # Create a unique string from the combination of values
        # This ensures the same incident always gets the same identifier
        unique_string = f"wildweb.incident.{center_code}.{incident_number}.{incident_name}.{incident_status}"
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_string))
    
    def _extract_state_from_code(self, center_code):
        """
        Extract state from center code.
        WildWeb center codes typically start with the state abbreviation.
        
        Args:
            center_code (str): Center code (e.g., 'CAANCC')
            
        Returns:
            str: State abbreviation
        """
        if center_code and len(center_code) >= 2:
            return center_code[:2]
        return ""
    
    def get_dispatch_centers_from_html(self, html_content):
        """
        Parse dispatch centers from HTML content.
        
        Args:
            html_content (str): HTML content of the WildWeb page
            
        Returns:
            list: List of dispatch center dictionaries
        """
        centers = []
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find the table containing dispatch centers
            center_table = soup.find('table', {'border': '1'})
            
            if not center_table:
                logger.error("Could not find dispatch center table")
                return []
            
            # Get all rows from the table (skip the header row)
            rows = center_table.find_all('tr')
            
            for row in rows[1:]:  # Skip header row
                cells = row.find_all('td')
                if len(cells) >= 3:  # Ensure we have at least 3 cells (name, status, link)
                    center_name = cells[0].text.strip()
                    status = cells[1].text.strip()
                    
                    # Get the link element
                    link_element = cells[2].find('a')
                    if link_element:
                        center_code = link_element.text.strip()
                        center_url = link_element.get('href')
                        
                        # Extract state from center code (first two letters)
                        state = self._extract_state_from_code(center_code)
                        
                        # Generate deterministic UUID for this center
                        center_id = self._generate_center_uuid(center_code)
                        
                        center_info = {
                            'id': center_id,
                            'center_code': center_code,
                            'center_name': center_name,
                            'state': state,
                            'status': status,
                            'url': center_url,
                            'last_updated': datetime.now().isoformat()
                        }
                        centers.append(center_info)
            
            logger.info(f"Found {len(centers)} dispatch centers")
            return centers
            
        except Exception as e:
            logger.error(f"Error parsing dispatch centers from HTML: {str(e)}")
            return []
    
    def get_dispatch_centers(self):
        """
        Scrape the list of dispatch centers from WildWeb.
        
        Returns:
            list: List of dictionaries containing dispatch center info
        """
        try:
            logger.info(f"Requesting dispatch centers from {self.base_url}")
            response = self.session.get(self.base_url)
            response.raise_for_status()
            
            return self.get_dispatch_centers_from_html(response.text)
            
        except Exception as e:
            logger.error(f"Error getting dispatch centers: {str(e)}")
            return []
    
    def save_dispatch_centers(self, centers):
        """Save the dispatch centers to the database"""
        if not centers:
            logger.warning("No dispatch centers to save")
            return
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for center in centers:
            cursor.execute('''
            INSERT OR REPLACE INTO dispatch_centers 
            (id, center_code, center_name, state, status, url, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                center['id'],
                center['center_code'],
                center['center_name'],
                center['state'],
                center['status'],
                center['url'],
                center['last_updated']
            ))
        
        conn.commit()
        conn.close()
        logger.info(f"Saved {len(centers)} dispatch centers to database")
    
    def get_incidents_for_center(self, center_info):
        """Get incident data for a specific dispatch center using the API directly."""
        incidents = []
        processed_count = 0
        total_count = 0
        
        try:
            logger.info(f"Fetching incidents for {center_info['center_name']}")
            incidents_url = f"https://www.wildwebe.net/incidents?dc_Name={center_info['center_code']}"
            
            # First load the page to capture the right API endpoint and any authentication
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            
            # Enable network logging
            chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
            
            driver = webdriver.Chrome(options=chrome_options)
            
            try:
                driver.get(incidents_url)
                
                # Wait for network activity to occur
                time.sleep(10)
                
                # Process performance logs to find the API call and its details
                api_url = None
                request_headers = {}
                
                logs = driver.get_log('performance')
                for log in logs:
                    try:
                        log_entry = json.loads(log['message'])
                        if 'message' in log_entry and 'method' in log_entry['message']:
                            if log_entry['message']['method'] == 'Network.requestWillBeSent':
                                request = log_entry['message']['params']['request']
                                request_url = request['url']
                                
                                # Look for API requests to AWS API Gateway
                                if 'execute-api.us-west-2.amazonaws.com' in request_url and '/centers/' in request_url:
                                    api_url = request_url
                                    request_headers = request.get('headers', {})
                                    logger.info(f"Found API endpoint: {api_url}")
                                    # Add to the code to save this endpoint for future use
                                    self.api_endpoints[center_info['center_code']] = api_url
                                    self._save_api_endpoint(center_info['center_code'], api_url)
                                    break
                    except Exception as e:
                        logger.debug(f"Error processing log entry: {str(e)}")
                
                if not api_url:
                    logger.warning(f"Could not find API endpoint for {center_info['center_name']}")
                    return [], 0, 0
                    
                # Now make the API request directly with the same headers the browser used
                session = requests.Session()
                
                # Copy important headers from the browser request
                important_headers = ['authorization', 'x-api-key', 'x-amz-date', 'x-amz-security-token']
                headers = {k.lower(): v for k, v in request_headers.items() 
                        if k.lower() in important_headers or k.lower().startswith('x-')}
                
                # Add common headers
                headers.update({
                    'User-Agent': driver.execute_script('return navigator.userAgent'),
                    'Referer': incidents_url,
                    'Origin': 'https://www.wildwebe.net'
                })
                
                logger.info(f"Making API request to {api_url}")
                response = session.get(api_url, headers=headers)
                
                if response.status_code == 200:
                    try:
                        if response is not None:
                            json_data = response.json()
                            data = []
                            for resp in json_data:
                                if resp.get('data') is not None:
                                    data.extend(resp['data'])
                        else:
                            data = []

                        total_count = len(data)
                        logger.info(f"API request succeeded, found {total_count} incidents")
                        
                        # Process the API response data
                        for item in data:
                            try:
                                processed_count += 1
                                
                                for key, value, in item.items():
                                    if isinstance(value, str) and value.startswith('*'):
                                        item[key] = None

                                # Map API fields based on the provided JSON structure
                                incident_number = item.get('inc_num', 'none')
                                incident_name = item.get('name', 'none')
                                incident_status = 'Unknown'  # Default status

                                # Parse fire_status JSON string if present
                                if item.get('fire_status'):
                                    try:
                                        fire_status = json.loads(item.get('fire_status', '{}'))
                                        # Determine incident status based on dates
                                        if fire_status.get('out'):
                                            incident_status = 'Out'
                                        elif fire_status.get('control'):
                                            incident_status = 'Controlled'
                                        elif fire_status.get('contain'):
                                            incident_status = 'Contained'
                                        else:
                                            incident_status = 'Active'
                                    except:
                                        # If JSON parsing fails, use a default status
                                        incident_status = 'Unknown'
                                
                                # Skip empty records
                                if not incident_number and not incident_name:
                                    continue
                                    
                                # Generate deterministic UUID
                                incident_id = self._generate_deterministic_incident_uuid(
                                    center_info['center_code'],
                                    incident_number,
                                    incident_name,
                                    incident_status
                                )
                                
                                # Generate a random UUID for this occurrence
                                occurrence_id = str(uuid.uuid4())
                                
                                # Extract coordinates if available
                                latitude = self._convert_float(item.get('latitude'))
                                longitude = self._convert_float(item.get('longitude'))
                                longitude = -longitude if longitude is not None else None
                                
                                # Parse resources array to string
                                resources = []
                                if item.get('resources'):
                                    resources = [r for r in item.get('resources', []) if r]
                                resources_str = ', '.join(resources) if resources else ''
                                
                                # Parse fiscal data if available
                                fiscal_data = {}
                                if item.get('fiscal_data'):
                                    try:
                                        fiscal_data = json.loads(item.get('fiscal_data', '{}'))
                                    except:
                                        pass
                                
                                fiscal_code = fiscal_data.get('fire_code')
                                wfdssunit = fiscal_data.get('wfdssunit')
                                
                                # Map the rest of the fields
                                incident_data = {
                                    'id': occurrence_id,
                                    'center_id': center_info['id'],
                                    'incident_id': incident_id,
                                    'incident_number': self._convert_int(incident_number),
                                    'fire_number': self._convert_int(item.get('fire_num')),
                                    'incident_uuid': item.get('uuid'),
                                    'fiscal': fiscal_code,
                                    'wfdssunit': wfdssunit,
                                    'incident_command': item.get('ic'),
                                    'incident_name': incident_name,
                                    'incident_type': item.get('type'),
                                    'incident_status': incident_status,
                                    'local_date': item.get('date'),
                                    'location': item.get('location'),
                                    'latitude': latitude,
                                    'longitude': longitude,
                                    'resources': resources_str,
                                    'acres': self._convert_float(item.get('acres')),
                                    'fuels': item.get('fuels'),
                                    'comments': item.get('webComment'),
                                    'raw_data': json.dumps(item),
                                    'ingest_date': datetime.now().isoformat()
                                }
                                
                                incidents.append(incident_data)
                            except Exception as e:
                                processed_count -= 1
                                logger.error(f"Error processing incident: {str(e)}")
                        
                        logger.info(f"Successfully processed {len(incidents)}/{total_count} incidents")
                        return incidents, processed_count, total_count
                        
                    except Exception as e:
                        logger.error(f"Error processing API response: {str(e)}")
                else:
                    logger.error(f"API request failed with status code {response.status_code}: {response.text}")
            
            finally:
                driver.quit()
                
        except Exception as e:
            logger.error(f"Error in API-based scraping for {center_info['center_name']}: {str(e)}")
        
        return [], processed_count, total_count
        
    def _convert_datetime(self, text):
        """Convert datetime string to datetime object"""
        if not text:
            return None

        # Try to parse the date and time
        try:
            formats = [
                "%m/%d/%y %H%M",  # 04/16/25 1221
                "%m/%d/%Y %H:%M", # 04/16/2025 12:21
                "%Y-%m-%d %H:%M:%S",
                "%m/%d/%Y"        # 04/16/2025
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(text, fmt)
                    return dt.isoformat()
                except ValueError:
                    continue
                    
            # If no formats matched, return the original string
            return text
            
        except Exception:
            return text
    
    def _convert_float(self, text):
        """Convert float string to float"""
        if not text:
            return None
        try:
            return float(text)
        except Exception:
            return None

    def _convert_int(self, text):
        """Convert int string to int"""
        if not text:
            return None
        try:
            return int(text)

        except Exception:
            return None
    
    def save_incidents(self, incidents):
        """Save incidents to the database"""
        if not incidents:
            logger.warning("No incidents to save")
            return
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for incident in incidents:
            cursor.execute('''
            INSERT INTO incidents 
            (id, center_id, incident_id, incident_number, fire_number, incident_uuid, 
             fiscal, wfdssunit, incident_command, incident_name, incident_type, 
             incident_status, local_date, location, 
             latitude, longitude, resources, acres, fuels, comments,
             raw_data, ingest_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(incident_id) DO UPDATE SET
                center_id = excluded.center_id,
                incident_number = excluded.incident_number,
                fire_number = excluded.fire_number,
                incident_uuid = excluded.incident_uuid,
                fiscal = excluded.fiscal,
                wfdssunit = excluded.wfdssunit,
                incident_command = excluded.incident_command,
                incident_name = excluded.incident_name,
                incident_type = excluded.incident_type,
                incident_status = excluded.incident_status,
                local_date = excluded.local_date,
                location = excluded.location,
                latitude = excluded.latitude,
                longitude = excluded.longitude,
                resources = excluded.resources,
                acres = excluded.acres,
                fuels = excluded.fuels,
                comments = excluded.comments,
                raw_data = excluded.raw_data,
                ingest_date = excluded.ingest_date
            ''', (
                incident['id'],
                incident['center_id'],
                incident['incident_id'],
                incident['incident_number'],
                incident['fire_number'],
                incident['incident_uuid'],
                incident['fiscal'],
                incident['wfdssunit'],
                incident['incident_command'],
                incident['incident_name'],
                incident['incident_type'],
                incident['incident_status'],
                incident['local_date'],
                incident['location'],
                incident['latitude'],
                incident['longitude'],
                incident['resources'],
                incident['acres'],
                incident['fuels'],
                incident['comments'],
                incident['raw_data'],
                incident['ingest_date']
            ))
        
        conn.commit()
        conn.close()
        logger.info(f"Saved {len(incidents)} incidents to database")
    
    def parse_centers_from_pasted_html(self, html_content):
        """
        Special method to parse directly pasted HTML content.
        
        Args:
            html_content (str): HTML content
            
        Returns:
            list: List of parsed dispatch centers
        """
        return self.get_dispatch_centers_from_html(html_content)
    
    def get_state_summary(self):
        """Get a summary of centers by state"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
        SELECT state, COUNT(*) as center_count 
        FROM dispatch_centers 
        GROUP BY state 
        ORDER BY state
        """)
        
        state_counts = cursor.fetchall()
        conn.close()
        
        return state_counts
    
    def get_incident_history(self, incident_id):
        """
        Get the history of a specific incident by its deterministic incident_id.
        
        Args:
            incident_id (str): The deterministic incident UUID
            
        Returns:
            list: List of all occurrences of this incident, ordered by scrape date
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
        SELECT * FROM incidents 
        WHERE incident_id = ? 
        ORDER BY ingest_date DESC
        """, (incident_id,))
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return results
    
    def run(self, from_html=None):
        """
        Run the full scraping process
        
        Args:
            from_html (str, optional): HTML content to parse instead of fetching from URL
        """
        logger.info("Starting WildWeb data scraping process")
        
        # Get and save dispatch centers
        if from_html:
            centers = self.parse_centers_from_pasted_html(from_html)
        else:
            centers = self.get_dispatch_centers()
            
        self.save_dispatch_centers(centers)
        
        # Get center IDs from database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT id, center_code, center_name, state, url FROM dispatch_centers")
        center_rows = cursor.fetchall()
        conn.close()
        
        # For each center, get and save incidents
        for center_row in center_rows:
            center_info = {
                'id': center_row[0],
                'center_code': center_row[1],
                'center_name': center_row[2],
                'state': center_row[3],
                'url': center_row[4]
            }
            
            # Add retry logic - try up to 5 times if we get 0 incidents
            max_retries = 5
            retry_count = 0
            incidents = []
            
            while retry_count < max_retries:
                incidents, processed_count, total_count = self.get_incidents_for_center(center_info)

                if (incidents and len(incidents) > 0) and (processed_count == total_count):
                    # We got incidents, no need to retry
                    logger.info(f"Successfully fetched {len(incidents)} incidents for {center_info['center_name']}")
                    break
                
                # If we get here, we got 0 incidents - retry after a delay
                retry_count += 1
                if retry_count < max_retries:
                    delay = retry_count * 5  # Increasing delay: 5s, 10s, 15s, 20s
                    logger.warning(f"Processed {processed_count} incidents out of {total_count} for {center_info['center_name']} on attempt {retry_count}. Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error(f"Failed to fetch incidents for {center_info['center_name']} after {max_retries} attempts")
            
            # Save incidents if we got any after retries
            if incidents:
                self.save_incidents(incidents)
            
            # Add a small delay between centers to be respectful to the server
            time.sleep(2)
            
        logger.info("Completed WildWeb data scraping process")
        
        # Generate summary by state
        state_summary = self.get_state_summary()
        logger.info(f"Centers by state: {state_summary}")


def main():
    """Main entry point for the scraper"""

    
    parser = argparse.ArgumentParser(description='Scrape WildWeb wildfire data')
    parser.add_argument('--db', default='./data/db/wildweb.db', help='Path to SQLite database')
    parser.add_argument('--html', help='Path to HTML file to parse instead of fetching from URL')
    parser.add_argument('--history', help='Get history for a specific incident ID')
    
    args = parser.parse_args()
    
    # Create database directory if it doesn't exist
    db_dir = os.path.dirname(args.db)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
    
    # Create logs directory if it doesn't exist
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    scraper = WildWebScraper(args.db)
    
    if args.history:
        # Get and display history for a specific incident
        history = scraper.get_incident_history(args.history)
        print(f"Found {len(history)} history records for incident {args.history}")
        for i, record in enumerate(history):
            print(f"\nRecord {i+1} - {record['scrape_date']}:")
            print(f"  Status: {record['incident_status']}")
    elif args.html:
        with open(args.html, 'r', encoding='utf-8') as f:
            html_content = f.read()
        scraper.run(from_html=html_content)
    else:
        scraper.run()


if __name__ == "__main__":
    main()