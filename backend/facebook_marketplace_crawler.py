import requests
from bs4 import BeautifulSoup
import os
import time
import random
import re
import logging
import json
import datetime
import sys
from typing import Callable, Optional, Union
from logging import Logger
from playwright.sync_api import sync_playwright, Page, Locator, ElementHandle
from utils.csv_writer import UnifiedCSVWriter, CSV_FIELDS, ensure_sources_dir, cleanup_old_csvs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WebPage:
    def __init__(
        self: "WebPage",
        page: Page,
        logger: Optional[Logger] = None,
    ) -> None:
        self.page = page
        self.logger = logger

    def _parent_with_cond(
        self: "WebPage",
        element: Optional[Union[Locator, ElementHandle]],
        cond: Callable,
        ret: Union[Callable, int],
    ) -> str:
        """Finding a parent element

        Starting from `element`, finding its parents, until `cond` matches, then return the `ret`th children,
        or a callable.
        """
        if element is None:
            return ""
        # get up at the DOM level, testing the children elements with cond,
        # apply the res callable to return a string
        parent: Optional[ElementHandle] = (
            element.element_handle() if isinstance(element, Locator) else element
        )
        # look for parent of approximate_element until it has two children and the first child is the heading
        while parent:
            children = parent.query_selector_all(":scope > *")
            if cond(children):
                if isinstance(ret, int):
                    return children[ret].text_content()
                else:
                    return ret(children)
            parent = parent.query_selector("xpath=..")
        raise ValueError("Could not find parent element with condition.")


class FacebookRegularItemPage():
    def __init__(
        self: "WebPage",
        page: Page,
        logger: Optional[Logger] = None,
    ):
        self.page = page
        self.logger = logger

    def get_title(self: "FacebookRegularItemPage") -> str:
        try:
            h1_element = self.page.query_selector_all("h1")[-1]
            return h1_element.text_content() or "**unspecified**"
        except KeyboardInterrupt:
            raise
        except Exception as e:
            if self.logger:
                self.logger.debug(f"{e}")
            return ""

    def get_price(self: "FacebookRegularItemPage") -> str:
        try:
            price_element = self.page.locator("h1 + *")
            return price_element.text_content() or "**unspecified**"
        except KeyboardInterrupt:
            raise
        except Exception as e:
            if self.logger:
                self.logger.debug(f"{e}")
            return ""

    def get_image_url(self: "FacebookRegularItemPage") -> str:
        try:
            image_url = self.page.locator("img").first.get_attribute("src") or ""
            return image_url
        except KeyboardInterrupt:
            raise
        except Exception as e:
            if self.logger:
                self.logger.debug(f"{e}")
            return ""

    def get_seller(self: "FacebookRegularItemPage") -> str:
        try:
            seller_link = self.page.locator("//a[contains(@href, '/marketplace/profile')]").last
            return seller_link.text_content() or "**unspecified**"
        except KeyboardInterrupt:
            raise
        except Exception as e:
            if self.logger:
                self.logger.error(
                    f"get_seller failed: {type(e).__name__}: {e}"
                )
            return ""

    def get_description(self: "FacebookRegularItemPage") -> str:
        try:
            # Find the span with text "condition", then parent, then next...
            description_element = self.page.locator(
                f'span:text("Condition") >> xpath=ancestor::ul[1] >> xpath=following-sibling::*[1]'
            )
            return description_element.text_content() or "**unspecified**"
        except KeyboardInterrupt:
            raise
        except Exception as e:
            if self.logger:
                self.logger.debug(f"{e}")
            return ""

    def parse(self: "FacebookItemPage", post_url: str):
        res = {
            "id": post_url.split("?")[0].rstrip("/").split("/")[-1],
            "title": self.get_title(),
            "image": self.get_image_url(),
            "description": self.get_description(),
            "seller": self.get_seller(),
        }
        print(res)

        return res


class FacebookSearchResultPage(WebPage):
    def _get_listing_elements_by_traversing_header(self: "FacebookSearchResultPage"):
        heading = self.page.locator(
            f'[aria-label="Collection of Marketplace items"]'
        )
        if not heading:
            return []

        grid_items = heading.locator(
            ":scope > :first-child > :first-child > :nth-child(3) > :first-child > :nth-child(2) > div"
        )
        # find each listing
        valid_listings = []
        try:
            for listing in grid_items.all():
                if not listing.text_content():
                    continue
                valid_listings.append(listing.element_handle())
        except Exception as e:
            # this error should be tolerated
            if self.logger:
                self.logger.debug(
                    f"Some grid item cannot be read: {e}"
                )
        return valid_listings

    def get_listings(self):
        # if no result is found
        btn = self.page.locator(f"""span:has-text('Browse Marketplace')""")
        if btn.count() > 0:
            if self.logger:
                msg = self._parent_with_cond(
                    btn.first,
                    lambda x: len(x) == 3
                    and 'Browse Marketplace' in (x[-1].text_content() or ""),
                    1,
                )
                self.logger.info(f"{msg}")
            return []

        # find the grid box
        try:
            valid_listings = self._get_listing_elements_by_traversing_header() or []
        except KeyboardInterrupt:
            raise
        except Exception as e:
            filename = datetime.datetime.now().strftime("debug_%Y%m%d_%H%M%S.html")
            if self.logger:
                self.logger.error(
                    f"failed to parse searching result. Page saved to {filename}: {e}"
                )
            with open(filename, "w", encoding="utf-8") as f:
                f.write(self.page.content())
            return []

        listings = []
        for idx, listing in enumerate(valid_listings):
            try:
                atag = listing.query_selector(
                    ":scope > :first-child > :first-child > :first-child > :first-child > :first-child > :first-child > :first-child > :first-child"
                )
                if not atag:
                    continue
                post_url = atag.get_attribute("href") or ""
                details_divs = atag.query_selector_all(":scope > :first-child > div")
                if not details_divs:
                    continue
                details = details_divs[1]
                divs = details.query_selector_all(":scope > div")
                raw_price = "" if len(divs) < 1 else divs[0].text_content() or ""
                title = "" if len(divs) < 2 else divs[1].text_content() or ""
                # location can be empty in some rare cases
                location = "" if len(divs) < 3 else (divs[2].text_content() or "")

                # get image
                img = listing.query_selector("img")
                image = img.get_attribute("src") if img else ""
                price = extract_price(raw_price)

                if post_url.startswith("/"):
                    post_url = f"https://www.facebook.com{post_url}"

                if image.startswith("/"):
                    image = f"https://www.facebook.com{image}"

                
                listings.append(
                    {
                        'id': post_url.split("?")[0].rstrip("/").split("/")[-1],
                        'title': title,
                        'image': image,
                        'price': price,
                        'post_url': post_url,
                        'location': location,
                    }
                )
            except KeyboardInterrupt:
                raise
            except Exception as e:
                if self.logger:
                    self.logger.error(
                        f"Failed to parse search results {idx + 1} listing: {e}"
                    )
                continue
        return listings

def extract_price(price: str) -> str:
    if not price or price == "**unspecified**":
        return price

    # extract leading non-numeric characters as currency symbol
    matched = re.match(r"(\D*)\d+", price)
    if matched:
        currency = matched.group(1).strip()
    else:
        currency = "$"

    matches = re.findall(currency.replace("$", r"\$") + r"[\d,]+(?:\.\d+)?", price)
    if matches:
        return " | ".join(matches[:2])
    return price

class FacebookMarketplaceCrawler:
    def __init__(self, logger):
        self.logger = logger
        self.browser = None
        self.ctx = None
        self.page = None
        self._storage_state_path = "facebook_state.json"
        # CSV setup: write to a single public file and de-dup by id
        self.item_count = 0
        ensure_sources_dir()
        cleanup_old_csvs()
        self.csv_path = os.path.join(
            ensure_sources_dir(),
            "facebook_marketplace.csv",
        )
        # Prepare unified CSV writer (append + dedupe)
        self.csv_writer = UnifiedCSVWriter(self.csv_path, CSV_FIELDS)

    def save_to_csv(self, item: dict) -> bool:
        try:
            # Add crawl_time automatically; writer also ensures it
            item = dict(item)
            item.setdefault(
                "crawl_time", datetime.datetime.now().isoformat(timespec="seconds")
            )
            if not self.csv_writer.write_if_new(item):
                if self.logger:
                    self.logger.info(
                        f"Skip duplicate listing ID: {item.get('id','')}"
                    )
                return False
            self.item_count += 1
            if self.logger:
                self.logger.info(
                    f"Saved listing ID: {item.get('id','')} to CSV. Total: {self.item_count}"
                )
            return True
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error saving to CSV: {e}")
            return False

    def close(self):
        try:
            if self.csv_writer:
                self.csv_writer.close()
                if self.logger:
                    self.logger.info("CSV file closed")
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error closing CSV: {e}")

    def crawl_pages(self):
        try:
            self.search()
        finally:
            self.close()

    def goto_url(self, url: str, attempt: int = 0) -> None:
        last_err = None
        for i in range(attempt, 11):
            try:
                assert self.page is not None
                # Use finite timeout and wait until DOM is ready
                self.page.goto(url, wait_until="domcontentloaded", timeout=60000)
                print("Loaded")
                return
            except KeyboardInterrupt:
                raise
            except Exception as e:
                last_err = e
                # If page crashed or target closed, recreate the page before retrying
                msg = str(e)
                if self.logger:
                    self.logger.warning(f"Navigation attempt {i+1} failed: {msg}")
                try:
                    if self.page and not self.page.is_closed():
                        self.page.close()
                except Exception:
                    pass
                try:
                    if self.ctx is not None:
                        self.page = self.ctx.new_page()
                        # Optional: keep timeouts consistent on new pages
                        try:
                            self.page.set_default_navigation_timeout(60000)
                            self.page.set_default_timeout(60000)
                        except Exception:
                            pass
                except Exception:
                    # If context is unusable, try to recreate it from the existing browser
                    if self.browser is not None:
                        try:
                            self.ctx = self.browser.new_context(storage_state=self._storage_state_path)
                            self.page = self.ctx.new_page()
                        except Exception:
                            pass
                time.sleep(5)
        # Exceeded attempts
        raise RuntimeError(
            f"Failed to navigate to {url} after 10 attempts. {last_err}"
        ) from last_err

    def search(self):
        with sync_playwright() as p:
            # Launch with sensible defaults; keep references on the instance
            self.browser = p.chromium.launch(headless=False, args=[
                "--disable-gpu",
                "--disable-dev-shm-usage",
            ])
            self.ctx = self.browser.new_context(storage_state=self._storage_state_path)
            self.page = self.ctx.new_page()
            try:
                self.page.set_default_navigation_timeout(60000)
                self.page.set_default_timeout(60000)
            except Exception:
                pass

            marketplace_url = (
                "https://www.facebook.com/marketplace/111711568847056/search?daysSinceListed=1&query=sony&exact=false"
            )
            self.goto_url(marketplace_url)
            # self.goto_url('file:///Users/nhty/Documents/Projects/Hobby/SanDoCu/debug_20251102_220908.html')

            found_listings = FacebookSearchResultPage(
                self.page, self.logger
            ).get_listings()
            time.sleep(5)

            for listing in found_listings:
                print(listing['post_url'])
                details = self.get_listing_details(listing['post_url'])
                listing["seller"] = details.get("seller", "")
                listing["description"] = details.get("description", "")
                # Prefer detailed page image/title if available
                if details.get("image"):
                    listing["image"] = details["image"]
                if details.get("title"):
                    listing["title"] = details["title"]
                # Persist to CSV
                self.save_to_csv(listing)
                time.sleep(5)

                
    def parse_listing(self, page, post_url, logger):
        supported_facebook_item_layouts = [
            FacebookRegularItemPage,
        ]

        for page_model in supported_facebook_item_layouts:
            try:
                return page_model(page, logger).parse(post_url)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                logger.error(f"Error running crawler: {e}")
                continue
        return None
    
    def get_listing_details(
        self,
        post_url: str
    ) :
        assert post_url.startswith("https://www.facebook.com")
        self.goto_url(post_url)
        details = self.parse_listing(self.page, post_url, self.logger)
        if details is None:
            raise ValueError(
                f"Failed to get item details of listing {post_url}. "
                "The listing might be missing key information (e.g. seller) or not in English."
                "Please add option language to your marketplace configuration is the latter is the case. See https://github.com/BoPeng/ai-marketplace-monitor?tab=readme-ov-file#support-for-non-english-languages for details."
            )
        return details
    


def run_fb_crawler():
    try:
        crawler = FacebookMarketplaceCrawler(logger)
        crawler.crawl_pages()
        return True
    except Exception as e:
        logger.error(f"Error running crawler: {e}")
        return False
