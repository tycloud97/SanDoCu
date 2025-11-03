import requests
from bs4 import BeautifulSoup
import os
import time
import random
import re
import logging
from datetime import datetime

from utils.csv_writer import UnifiedCSVWriter, CSV_FIELDS, ensure_sources_dir, cleanup_old_csvs

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


class ChototCrawler:
    def __init__(self, start_page=1, end_page=1):
        """Initialize the crawler with page range and log ID."""
        self.start_page = start_page
        self.end_page = end_page

        # Prepare unified single-file output under frontend/public with de-dup
        ensure_sources_dir()
        cleanup_old_csvs()
        self.csv_path = os.path.join(ensure_sources_dir(), "chotot.csv")
        self.csv_writer = UnifiedCSVWriter(self.csv_path, CSV_FIELDS)

        # Base URL for requests
        self.base_url = "https://www.chotot.com/mua-ban-do-dien-tu-da-nang"
        # Counter for items found
        self.item_count = 0

    

    def get_page(self, url):
        """Fetch a page with retry logic."""
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                # Add a small delay to avoid being blocked
                time.sleep(random.uniform(0.5, 1.5))

                # Use a random User-Agent
                user_agents = [
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
                ]

                headers = {
                    "User-Agent": random.choice(user_agents),
                    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
                    "Referer": "https://chotot.com/",
                }

                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()

                logger.info(f"Response received from {url}: {len(response.text)} bytes")

                return response.text
            except requests.exceptions.RequestException as e:
                retry_count += 1
                wait_time = retry_count * 2
                logger.warning(
                    f"Error fetching {url}: {e}. Retrying in {wait_time} seconds..."
                )
                time.sleep(wait_time)

        logger.error(f"Failed to fetch {url} after {max_retries} attempts")
        return None

    def parse_price(self, price_text):
        """Parse price text to integer."""
        if not price_text:
            return None

        # Remove non-digit characters
        price_number = re.sub(r"[^\d]", "", price_text)
        if price_number:
            return int(price_number)
        return None


    def extract_id(self, url):
        """Extract car ID from URL."""
        match = re.search(r"/(\d+)\.htm", url)
        if match:
            return match.group(1)
        return None

    def extract_listing_urls(self, html_content):
        """Extract car listing URLs from the page."""
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, "html.parser")
        urls = []

        # Find all a tags with href matching listing detail pattern
        links = soup.find_all("a", href=re.compile(r"/mua-ban-.*-da-nang/\d+\.htm"))

        # Add URLs from direct link finding
        for link in links:
            href = link.get("href")
            if href:
                urls.append(href)

        # Convert to full URLs
        full_urls = []
        for url in urls:
            # Remove fragments
            url = url.split("#")[0]

            # Ensure full URL
            if url.startswith("//"):
                url = "https:" + url
            elif url.startswith("/"):
                url = "https://chotot.com" + url
            elif not url.startswith("http"):
                url = "https://chotot.com/" + url

            full_urls.append(url)

        # Remove duplicates
        unique_urls = list(set(full_urls))
        logger.info(f"Found {len(unique_urls)} unique car URLs")

        return unique_urls

    def extract_details(self, html_content, url):
        """Extract car details from detail page."""
        if not html_content:
            return None

        soup = BeautifulSoup(html_content, "html.parser")
        car_data = {}

        car_id = self.extract_id(url)
        car_data["id"] = car_id
        car_data["post_url"] = url

        # Extract title
        title_elem = soup.find("h1")
        if title_elem:
            car_data["title"] = title_elem.text.strip()

        # Extract price
        price_elem = soup.find("b", class_="p1mdjmwc")
        if price_elem:
            car_data["price"] = self.parse_price(price_elem.text)


        # Extract location
        location_elem = soup.find("span", class_="bwq0cbs flex-1")
        if location_elem:
            car_data["location"] = location_elem.text.strip()

        # Extract description
        description_elem = soup.find("p", class_="c90nk1b")
        if description_elem:
            car_data["description"] = description_elem.text.strip()

        # Extract seller name if available
        seller_link = soup.find("a", href=re.compile(r"/(user|nguoi-ban|profile|cua-hang)"))
        if seller_link and seller_link.text:
            car_data["seller"] = seller_link.text.strip()

        # Extract first image (prefer og:image)
        og_img = soup.find("meta", attrs={"property": "og:image"})
        if og_img and og_img.get("content"):
            car_data["image"] = og_img.get("content").strip()
        else:
            img = soup.find("img")
            if img and img.get("src"):
                car_data["image"] = img.get("src").strip()

        return car_data

    def save_to_csv(self, car_data):
        """Save car data to CSV file."""
        if not car_data or "id" not in car_data:
            logger.warning("Cannot save car: Invalid data")
            return False

        try:
            row = dict(car_data)
            row.setdefault("crawl_time", datetime.now().isoformat(timespec="seconds"))
            if not self.csv_writer.write_if_new(row):
                logger.info(f"Skip duplicate ID: {row.get('id','')}")
                return False
            self.item_count += 1
            logger.info(
                f"Saved item ID: {car_data['id']} to CSV. Total: {self.item_count}"
            )
            return True
        except Exception as e:
            logger.error(f"Error saving item to CSV: {e}")
            return False

    def crawl_page(self, page_num):
        """Crawl a single page of car listings."""
        page_url = f"{self.base_url}?f=p&page={page_num}"
        logger.info(f"Crawling page: {page_url}")

        # Get the page HTML
        page_html = self.get_page(page_url)
        if not page_html:
            logger.error(f"Could not get HTML from page {page_url}")
            return 0

        # Extract car URLs
        car_urls = self.extract_listing_urls(page_html)

        logger.info(f"Found {len(car_urls)} cars on page {page_num}")

        page_count = 0
        for idx, car_url in enumerate(car_urls):
            try:
                # Get car detail page
                car_html = self.get_page(car_url)
                if not car_html:
                    continue

                # Extract car details
                car_data = self.extract_details(car_html, car_url)
                if car_data:
                    # Save to CSV - function này sẽ tự động update records_count
                    if self.save_to_csv(car_data):
                        page_count += 1

                        # Log current count
                        logger.info(
                            f"Saved car: {car_data.get('title', 'Unknown')} - ID: {car_data.get('id', 'Unknown')} - Total: {self.item_count}"
                        )

                        # Print progress với số thực tế
                        print(
                            f"\rCars crawled: {self.item_count} (Page {page_num}, Item {idx + 1}/{len(car_urls)})",
                            end="",
                            flush=True,
                        )

            except Exception as e:
                logger.error(f"Error processing car {car_url}: {e}")

        # Page completed
        final_status = f"running-completed-page-{page_num}"

        return page_count

    def crawl_pages(self):
        """Crawl multiple pages in the specified range."""
        logger.info(f"Starting crawl from page {self.start_page} to {self.end_page}")

        total_cars = 0
        try:
            for page_num in range(self.start_page, self.end_page + 1):
                cars_on_page = self.crawl_page(page_num)
                total_cars += cars_on_page
                logger.info(f"Page {page_num}: Crawled {cars_on_page} cars")

                # Print total crawled cars
                print(f"\nTotal cars crawled: {self.item_count}")

                # Small delay between pages
                time.sleep(random.uniform(1, 2))

            logger.info(f"Crawl completed! Total cars: {total_cars}")

        except Exception as e:
            logger.error(f"Crawl error: {str(e)}")

            raise

        finally:
            self.close()

        return total_cars

    def close(self):
        """Close the CSV file."""
        if self.csv_writer:
            try:
                self.csv_writer.close()
            finally:
                logger.info("CSV file closed")


def run_crawler(start_page, end_page):
    try:
        crawler = ChototCrawler(start_page, end_page)
        crawler.crawl_pages()
        return True
    except Exception as e:
        logger.error(f"Error running crawler: {e}")
        return False


def get_latest_raw_file():
    """Get the path to the latest raw data file."""
    raw_dir = os.path.join("data", "raw")

    if not os.path.exists(raw_dir):
        return None

    files = [f for f in os.listdir(raw_dir) if f.endswith(".csv")]

    if not files:
        return None

    # Sort by modification time (newest first)
    files.sort(key=lambda f: os.path.getmtime(os.path.join(raw_dir, f)), reverse=True)

    return os.path.join(raw_dir, files[0])
