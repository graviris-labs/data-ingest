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
        # Set common headers to mimic browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml',
            'Accept-Language': 'en-US,en;q=0.9'
        })
        self._ensure_db_exists()
        
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
            incident_number TEXT,
            fiscal TEXT,
            incident_name TEXT,
            incident_type TEXT,
            incident_status TEXT,
            local_date TIMESTAMP,
            location TEXT,
            latitude REAL,
            longitude REAL,
            resources TEXT,
            acres REAL,
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
        """
        Get incident data for a specific dispatch center.
        This function handles MUI DataGrid with virtual scrolling.
        
        Args:
            center_info (dict): Dispatch center information with URL
            
        Returns:
            list: List of incident data for the center
        """
        incidents = []
        processed_rows = set()
        total_rows = 0
        try:
            logger.info(f"Fetching incidents for {center_info['center_name']}")
            
            # Update the URL to use the incidents page with the dc_Name parameter
            incidents_url = f"https://www.wildwebe.net/incidents?dc_Name={center_info['center_code']}"
            logger.info(f"Accessing incidents URL: {incidents_url}")
            
            # Set up headless Chrome browser
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            # Increase window size for more visible rows
            chrome_options.add_argument("--window-size=1920,1080")
            
            # Initialize the browser
            driver = webdriver.Chrome(options=chrome_options)
            try:
                # Navigate to the incidents page
                driver.get(incidents_url)
                
                # Wait for the data grid to load
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[role='grid']"))
                )
                
                # Give the JavaScript some time to load all the data
                time.sleep(10)
                
                grid_element = driver.find_element(By.CSS_SELECTOR, "[role='grid']")
                try:
                    aria_rowcount = grid_element.get_attribute('aria-rowcount')
                    if aria_rowcount and aria_rowcount.isdigit():
                        total_rows = int(aria_rowcount)
                        logger.info(f"Found total rows from aria-rowcount: {total_rows - 1}")
                except Exception as e:
                    logger.error(f"Error getting aria-rowcount: {str(e)}")
                
                # if total_rows == 0 and row_count_elements:
                #     for elem in row_count_elements:
                #         try:
                #             row_count_text = elem.text
                #             logger.info(f"Found row count element: {row_count_text}")
                            
                #             # Try to extract the number (format might be "1-10 of 42" or just "42 rows")
                #             matches = re.search(r'of\s+(\d+)|(\d+)\s+rows', row_count_text)
                #             if matches:
                #                 # Get the matching group that contains the number
                #                 total_rows = int(matches.group(1) if matches.group(1) else matches.group(2))
                #                 logger.info(f"Total rows identified: {total_rows}")
                #                 break
                #         except Exception as e:
                #             logger.error(f"Error extracting row count: {str(e)}")
                
                ## If we couldn't determine the row count from the UI, try executing JavaScript to get it
                # if total_rows == 0:
                #     try:
                #         # Try to get row count via JavaScript
                #         js_row_count = driver.execute_script("""
                #         // Try to get total row count from MUI DataGrid
                #         const gridElement = document.querySelector('[role="grid"]');
                #         if (gridElement && gridElement.__data && gridElement.__data.length) {
                #             return gridElement.__data.length;
                #         }
                        
                #         // Try to find in other places
                #         for (const key in window) {
                #             if (key.match(/data|rows|incidents|grid/i)) {
                #                 const value = window[key];
                #                 if (Array.isArray(value) && value.length > 0) {
                #                     // Check if it looks like our data
                #                     if (value[0] && (value[0].incident_num || value[0].name)) {
                #                         return value.length;
                #                     }
                #                 }
                #             }
                #         }
                        
                #         // Default
                #         return 0;
                #         """)
                        
                #         if js_row_count and isinstance(js_row_count, (int, float)) and js_row_count > 0:
                #             total_rows = int(js_row_count)
                #             logger.info(f"JavaScript found {total_rows} total rows")
                #     except Exception as e:
                #         logger.error(f"Error getting row count via JavaScript: {str(e)}")
                
                # Still no row count? Let's set a high default value to ensure we get all data
                if total_rows == 0:
                    # Set a high default - typical incidents might be in the hundreds for active centers
                    total_rows = 250  # This is a very high default to ensure we try to get all rows
                    logger.info(f"Could not determine row count, using default target of {total_rows} rows")
                
                # Find the scrollable container for the virtual scroller
                scroller = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".MuiDataGrid-virtualScroller"))
                )
                
                # Find the grid
                grid = driver.find_element(By.CSS_SELECTOR, "[role='grid']")
                
                # Get headers first
                header_row = grid.find_elements(By.CSS_SELECTOR, "[role='columnheader']")
                
                # Map headers to positions
                field_positions = {}
                for i, header in enumerate(header_row):
                    header_text = header.text.strip().lower()
                    if 'inc#' in header_text:
                        field_positions['incident_number'] = i
                    elif 'fiscal' in header_text:
                        field_positions['fiscal'] = i
                    elif 'name' in header_text:
                        field_positions['incident_name'] = i
                    elif 'type' in header_text:
                        field_positions['incident_type'] = i
                    elif 'status' in header_text:
                        field_positions['incident_status'] = i
                    elif 'local' in header_text or 'date' in header_text:
                        field_positions['local_date'] = i
                    elif 'location' in header_text:
                        field_positions['location'] = i
                    elif 'lat' in header_text or 'long' in header_text or 'lat/long' in header_text:
                        field_positions['lat_long'] = i
                    elif 'resources' in header_text:
                        field_positions['resources'] = i
                    elif 'acres' in header_text:
                        field_positions['acres'] = i
                    elif 'web' in header_text or 'comment' in header_text:
                        field_positions['comments'] = i
                
                # If we couldn't determine field positions, use a default mapping
                if not field_positions:
                    # Create a default mapping based on common column order
                    field_positions = {
                        'incident_number': 0,
                        'fiscal': 1,
                        'incident_name': 2,
                        'incident_type': 3,
                        'incident_status': 4,
                        'local_date': 5,
                        'location': 6,
                        'lat_long': 7,
                        'resources': 8,
                        'acres': 9,
                        'comments': 10
                    }
                
                # Initialize a set to track which rows we've already processed
                processed_rows = set()
                
                # Set target rows to the determined total
                target_rows = total_rows
                logger.info(f"Attempting to extract {target_rows - 1} rows through scrolling")
                
                # Keep track of the maximum row index we've seen
                max_row_index_seen = 0
                
                # Keep scrolling and processing rows until we have all rows or reached a limit
                # Use a higher max_scroll_attempts value to ensure we can get to hundreds of rows if needed
                max_scroll_attempts = 100  # Increased from 50 to 100
                scroll_attempts = 0
                stagnant_count = 0  # Counter for when no new rows are found
                last_row_count = 0
                seen_incident_ids = set()

                while len(processed_rows) < target_rows and scroll_attempts < max_scroll_attempts:
                    # Scroll down to load more rows
                    actions = ActionChains(driver)
                    actions.move_to_element(scroller)
                    actions.click()
                    actions.send_keys(Keys.PAGE_DOWN)
                    actions.perform()

                    current_row_count = len(processed_rows)
                    # Wait a moment for new rows to load
                    time.sleep(0.5)
                    
                    # Get all visible rows
                    visible_rows = grid.find_elements(By.CSS_SELECTOR, "[role='row'][data-rowindex]")

                    # Process any new rows
                    for row in visible_rows:
                        try:
                            # Get row index to uniquely identify it
                            row_index = row.get_attribute('data-rowindex')
                            
                            # Skip header row
                            if 'headerrow' in (row.get_attribute('class') or ''):
                                continue
                            
                            # Skip already processed rows
                            if row_index in processed_rows:
                                continue
                            
                            # Add debug logging to track row indices
                            logger.debug(f"Processing row with index: {row_index}")
                            
                            # Mark this row as processed
                            processed_rows.add(row_index)
                            
                            # Update max row index seen
                            try:
                                row_index_int = int(row_index)
                                max_row_index_seen = max(max_row_index_seen, row_index_int)
                            except ValueError:
                                pass
                            
                            # Get all cells in this row
                            cells = row.find_elements(By.CSS_SELECTOR, "[role='cell']")
                            
                            # Add debug logging for empty rows
                            if not cells:
                                logger.debug(f"Row {row_index} has no cells")
                                continue
                            
                            # Extract text from cells based on field positions
                            cell_values = {}
                            for field, position in field_positions.items():
                                if position < len(cells):
                                    cell_values[field] = cells[position].text.strip()
                                else:
                                    cell_values[field] = ""
                            
                            # Add more debug logging for cell values
                            logger.debug(f"Row {row_index} cell values: {cell_values}")
                            
                            # Extract key fields
                            incident_number = cell_values.get('incident_number', '')
                            incident_name = cell_values.get('incident_name', '')
                            incident_status = cell_values.get('incident_status', 'none')
                            lat_long = self._extract_lat_long(cell_values.get('lat_long', ''))
                            latitude = lat_long[0] if lat_long else None
                            longitude = lat_long[1] if lat_long else None

                            # Skip empty rows
                            if not incident_number and not incident_name:
                                logger.debug(f"Skipping row {row_index} because it has no incident number or name")
                                continue
                            
                            # Generate deterministic UUID to identify the same incident
                            incident_id = self._generate_deterministic_incident_uuid(
                                center_info['center_code'], 
                                incident_number, 
                                incident_name,
                                incident_status
                            )
                            
                            if incident_id in seen_incident_ids:
                                logger.debug(f"Duplicate incident ID found: {incident_id}")
                                continue
                            
                            # Add incident_id to seen_incident_ids
                            seen_incident_ids.add(incident_id)

                            # Generate a random UUID for this specific occurrence
                            occurrence_id = str(uuid.uuid4())
                            
                            # Create incident data
                            incident_data = {
                                'id': occurrence_id,
                                'incident_id': incident_id,
                                'center_id': center_info['id'],
                                'incident_number': incident_number,
                                'incident_name': incident_name,
                                'fiscal': cell_values.get('fiscal', ''),
                                'incident_type': cell_values.get('incident_type', ''),
                                'incident_status': cell_values.get('incident_status', ''),
                                'local_date': self._convert_datetime(cell_values.get('local_date', '')),
                                'location': cell_values.get('location', ''),
                                'latitude': latitude,
                                'longitude': longitude,
                                'resources': cell_values.get('resources', ''),
                                'acres': self._extract_acres(cell_values.get('acres', '')),
                                'comments': cell_values.get('comments', ''),
                                'raw_data': json.dumps({field: value for field, value in cell_values.items()}),
                                'ingest_date': datetime.now().isoformat()
                            }
                            # Add to our list
                            incidents.append(incident_data)
                            
                        except Exception as e:
                            logger.error(f"Error processing row: {str(e)}")
                    
                    # Check if we got new rows in this iteration
                    if len(processed_rows) > current_row_count:
                        last_row_count = len(processed_rows)
                        logger.info(f"Processed {len(processed_rows)} rows so far, highest row index: {max_row_index_seen}")
                        stagnant_count = 0  # Reset stagnant counter
                    else:
                        # If no new rows found in this iteration, count as stagnant
                        stagnant_count += 1
                        
                        # If we're stagnant for too long, try more aggressive scrolling
                        if stagnant_count >= 3:
                            # Try JavaScript scrolling to different positions
                            scroll_percentage = (scroll_attempts % 10) / 10  # Vary between 0 and 0.9
                            try:
                                # Try to scroll to different positions in the grid
                                driver.execute_script(
                                    f"arguments[0].scrollTop = arguments[0].scrollHeight * {scroll_percentage}", 
                                    scroller
                                )
                                time.sleep(1)  # Give it more time to load
                            except:
                                # If JavaScript scrolling fails, try End key or arrow keys
                                keys_to_try = [Keys.END, Keys.PAGE_DOWN, Keys.ARROW_DOWN * 10]
                                key_to_send = keys_to_try[scroll_attempts % len(keys_to_try)]
                                
                                actions = ActionChains(driver)
                                actions.move_to_element(scroller)
                                actions.click()
                                actions.send_keys(key_to_send)
                                actions.perform()
                                time.sleep(1)
                    
                    # Increment scroll attempts counter
                    scroll_attempts += 1
                    
                    # Log progress periodically
                    if scroll_attempts % 10 == 0:
                        logger.info(f"Scroll attempt {scroll_attempts}, processed {len(processed_rows)} rows")
                    
                    if len(processed_rows) < total_rows - 1:
                        logger.warning(f"Only processed {len(processed_rows)} of {total_rows} rows")
                        
                        # Find missing row indices
                        expected_indices = set(range(total_rows))
                        processed_indices = set()
                        
                        # Convert string indices to integers
                        for idx in processed_rows:
                            try:
                                processed_indices.add(int(idx))
                            except ValueError:
                                pass
                        
                        # Calculate missing indices
                        missing_indices = expected_indices - processed_indices
                        logger.warning(f"Missing row indices: {sorted(missing_indices)}")

                    if len(processed_rows) >= target_rows:
                        logger.warning(f"Scrolling got {len(processed_rows)} rows (100% of target {target_rows}) trying to load more...")
                        target_rows += 50

                    if (len(processed_rows) > target_rows * 0.9 and stagnant_count >= 15) or (stagnant_count >= 20):
                        logger.info(f"Either over 90% of target rows processed ({len(processed_rows)}/{target_rows}) and no new rows for 15 attempts, or no new rows for 20 attempts. Assuming all rows loaded.")
                        break

                    # # Special termination case: if we've seen more than 50% of the target rows, 
                    # # and we've been stagnant for a while, assume we've loaded all rows
                    # if (len(processed_rows) > target_rows * 0.5 and stagnant_count >= 10):
                    #     logger.info(f"Over 50% of target rows processed ({len(processed_rows)}/{target_rows}) and no new rows for 10 attempts. Assuming all rows loaded.")
                    #     break

                    if (len(processed_rows) == 0 and stagnant_count >= 15):
                        logger.error(f"No rows for 10 attempts. Check data source: {center_info} and try again.")
                        for log in driver.get_log('browser'):
                            logger.error(f"Browser console: {log}")
                        break
                
                logger.info(f"Completed scrolling after {scroll_attempts} attempts, extracted {len(incidents)} incidents")
                
            except Exception as e:
                logger.error(f"Selenium error: {str(e)}")
                
            finally:
                # Close the browser
                driver.quit()
            
            # If we got incidents, return them
            if not incidents:
                logger.warning(f"No incidents found for {center_info['center_name']}")
            
            return incidents, len(processed_rows), target_rows - 1
        except Exception as e:
            logger.error(f"Error getting incidents for {center_info['center_name']}: {str(e)}")
            return []
    
    def _get_cell_value(self, cells, header_map, field_name):
        """Get cell value based on header map"""
        if field_name in header_map and header_map[field_name] < len(cells):
            return cells[header_map[field_name]].text.strip()
        return ""
    
    def _extract_acres(self, text):
        """Extract acreage value from text"""
        if not text:
            return None
            
        # Remove non-numeric characters except decimal points
        numbers = re.findall(r'[\d,]+\.?\d*', text)
        if numbers:
            # Convert to float, handling commas
            try:
                return float(numbers[0].replace(',', ''))
            except ValueError:
                return None
        return None
        
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

    def _extract_lat_long(self, text):
        """Extract latitude and longitude from text if available"""
        if not text:
            return None
            
        # Look for patterns like: 39.5432, -122.3456 or similar
        coords = re.findall(r'(-?\d+\.\d+)[,\s]+(-?\d+\.\d+)', text)
        if coords:
            try:
                return (float(coords[0][0]), float(coords[0][1]))
            except (ValueError, IndexError):
                return None
                
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
            (id, center_id, incident_id, incident_number, fiscal, incident_name, 
             incident_type, incident_status, local_date, location, 
             latitude, longitude, resources, acres, comments,
             raw_data, ingest_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(incident_id) DO UPDATE SET
                center_id = excluded.center_id,
                incident_number = excluded.incident_number,
                fiscal = excluded.fiscal,
                incident_name = excluded.incident_name,
                incident_type = excluded.incident_type,
                incident_status = excluded.incident_status,
                local_date = excluded.local_date,
                location = excluded.location,
                latitude = excluded.latitude,
                longitude = excluded.longitude,
                resources = excluded.resources,
                acres = excluded.acres,
                comments = excluded.comments,
                raw_data = excluded.raw_data,
                ingest_date = excluded.ingest_date
            ''', (
                incident['id'],
                incident['center_id'],           # Random UUID for this occurrence
                incident['incident_id'],  # Deterministic UUID to identify the same incident
                incident['incident_number'],
                incident['fiscal'],
                incident['incident_name'],
                incident['incident_type'],
                incident['incident_status'],
                incident['local_date'],
                incident['location'],
                incident['latitude'],
                incident['longitude'],
                incident['resources'],
                incident['acres'],
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
        ORDER BY scrape_date
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
                incidents, processed_rows, total_rows = self.get_incidents_for_center(center_info)
                
                if (incidents and len(incidents) > 0) and (processed_rows == total_rows):
                    # We got incidents, no need to retry
                    logger.info(f"Successfully fetched {len(incidents)} incidents for {center_info['center_name']}")
                    break
                
                # If we get here, we got 0 incidents - retry after a delay
                retry_count += 1
                if retry_count < max_retries:
                    delay = retry_count * 5  # Increasing delay: 5s, 10s, 15s, 20s
                    logger.warning(f"Found 0 incidents for {center_info['center_name']} on attempt {retry_count}. Retrying in {delay} seconds...")
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