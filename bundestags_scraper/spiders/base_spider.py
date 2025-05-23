import json
import logging
import pathlib
from urllib.parse import urlparse

from neo4j import GraphDatabase
from bundestags_scraper.items import SourcePageItem, SourceDomainItem

# --------------------------------------------------------------------------- #
# Logging helper                                                              #
# --------------------------------------------------------------------------- #
STATIC_LOG_DIR = pathlib.Path(__file__).resolve().parents[2] / "static_data" / "logs"
STATIC_LOG_DIR.mkdir(parents=True, exist_ok=True)

class LoggingMixin:
    """Two JSON loggers: normal + missing-field warnings."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        core_logger = logging.getLogger(self.name)
        if not core_logger.handlers:
            fmt = (
                '{"ts":"%(asctime)s","spider":"%(name)s","level":"%(levelname)s",'
                '"event":%(message)s}'
            )
            formatter = logging.Formatter(fmt, "%Y-%m-%dT%H:%M:%S")

            sh = logging.StreamHandler()
            sh.setFormatter(formatter)

            # file with all events
            fh_all = logging.FileHandler(
                STATIC_LOG_DIR / f"{self.name}.log", encoding="utf-8"
            )
            fh_all.setFormatter(formatter)

            core_logger.addHandler(sh)
            core_logger.addHandler(fh_all)
            core_logger.setLevel(logging.INFO)

        
        # ------------------------------------------------------------------
        # 2) Missing-field logger
        # ------------------------------------------------------------------
        self.missing_logger = logging.getLogger(f"{self.name}_missing")
        if not self.missing_logger.handlers:
            fh_missing = logging.FileHandler(
                STATIC_LOG_DIR / f"{self.name}_missing.log", encoding="utf-8"
            )
            fh_missing.setFormatter(formatter)
            fh_missing.setLevel(logging.WARNING)
            self.missing_logger.addHandler(fh_missing)
            self.missing_logger.setLevel(logging.WARNING)

    
    def log_event(self, level: str, event: str, **data):
        """Uniform JSON-encoded event writer."""
        payload = json.dumps({"event": event, **data}, ensure_ascii=False)
        getattr(self.logger, level)(payload)


    def log_missing(self, item, url, missing):
        """Write missing-field warning to dedicated file."""
        payload = json.dumps(
            {"url": url, "missing": missing, "item": dict(item)}, ensure_ascii=False
        )
        self.missing_logger.warning(payload)

class SourceMixin:
    """Everything that is page / domain related."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_domains = set()
    
    def get_domain(self, response):
        """
        Extract the domain name from a response URL.

        :param response: A Scrapy Response object.
        :return: The domain string, e.g. 'de.wikipedia.org'.
        """
        return urlparse(response.url).netloc

    def generate_source_domain_item(self, response):
        domain = self.get_domain(response)
        if domain not in self.seen_domains:
            self.seen_domains.add(domain)
            self.log_event("info", "yield_domain_item", domain=domain, url=response.url)
            return SourceDomainItem(
                item_type="domain",
                domain=domain,
                description=f"Domain extracted from {response.url}",
            )

    def generate_source_page_item(self, response):
        """
        Create and return a SourcePageItem for the current response.

        Captures the page URL, title, full HTML, and domain.

        :param response: A Scrapy Response object.
        :return: SourcePageItem.
        """
        domain = self.get_domain(response)
        self.log_event("debug", "yield_source_page_item", domain=domain, url=response.url)
        return SourcePageItem(
            item_type='page',
            url=response.url,
            title=response.xpath('normalize-space(//title/text())').get(),
            full_html=response.text,
            source_domain=domain
        )
        
    def add_source_page(self, response):
        """
        Add SourceDomainItem (only if first time seeing this domain),
        Add a SourcePageItem (always).
        """
        if (dom := self.generate_source_domain_item(response)):
            yield dom
        yield self.generate_source_page_item(response)
        
    def validate_item(self, item, mandatory, url):
        missing = [f for f in mandatory if not item.get(f)]
        if missing:
            self.log_missing(item, url, missing)
        return not missing
        
class Neo4jMixin:
    """Get a Neo4j driver from Scrapy settings (set in settings.py)."""
    
    # scrapy_project/base_spider.py
class Neo4jMixin():
    """
    Lazily opens a Neo4j driver.  Credentials are pulled *once* from
    crawler.settings in __init__, so they are ready for start_requests().
    """

    def __init__(self, *args, **kwargs):
        crawler = kwargs.pop("crawler", None)
        super().__init__(*args, **kwargs)

        if crawler is None:
            raise RuntimeError("Neo4jMixin must be instantiated via from_crawler()")

        s = crawler.settings
        self.neo4j_uri      = s.get("NEO4J_URI")
        self.neo4j_user     = s.get("NEO4J_USER")
        self.neo4j_password = s.get("NEO4J_PASSWORD")

        self._driver = None

    # ------------------------------------------------------------------
    def _init_driver(self):
        if not self._driver:
            #self.log_event("info", "neo4j_connect", uri=self.neo4j_uri)
            self._driver = GraphDatabase.driver(
                self.neo4j_uri, auth=(self.neo4j_user, self.neo4j_password)
            )
        return self._driver

    def neo4j_session(self):
        return self._init_driver().session()

    def close_spider(self, spider):
        if self._driver:
            #self.log_event("info", "neo4j_disconnect")
            self._driver.close()

    # ------------------------------------------------------------------
    # ensure Scrapy constructs the spider with `crawler` kw-arg
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        kwargs["crawler"] = crawler
        return cls(*args, **kwargs)
