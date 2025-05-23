import re
import scrapy
from .base_spider import SourceMixin, LoggingMixin
from bundestags_scraper.items import LegislativePeriodItem

class LegislativePeriodSpider(LoggingMixin, SourceMixin, scrapy.Spider):
    """
    Scrape links to each electoral period's member list.

    Yields LegislativePeriodItem, SourceDomainItem, and SourcePageItem.
    """
    name = "legislative_periods"
    allowed_domains = ["wikipedia.org", "bundestag.de"]
    start_urls = [
        "https://de.wikipedia.org/wiki/Liste_der_Listen_der_Mitglieder_des_Deutschen_Bundestages",
    ]

    def parse(self, response):
        """
        Parse the main list page for legislative periods.
        """
        for src in self.add_source_page(response):
            yield src
            
        self.log_event("info", "start_parse_list_of_lists", url=response.url)
        
        if "wikipedia.org" not in self.get_domain(response):
            self.log_event("debug", "skip_non_wiki", url=response.url)
            return

        
        rows = response.xpath("//div[contains(@class, 'mw-content-ltr mw-parser-output')]//ul//li")
        self.log_event("info", "found_rows", count=len(rows), url=response.url)
        
        for row in rows:
            a_tag = row.xpath('.//a')
            
            if a_tag:
                href = a_tag.xpath('@href').get()
                text = a_tag.xpath('normalize-space(text())').get()
                
                if href and text and 'Wahlperiode' in text:
                    m = re.search(r"\((\d+)\.\s*Wahlperiode\)", text)
                    
                    period = m.group(1) if m else None
                    
                    self.log_event("debug", "yield_period_item", period=period, name=text)
                    
                    yield LegislativePeriodItem(
                        item_type='legislative_period',
                        period_number=period,
                        name=text,
                        start_date=None,
                        end_date=None,
                        source_page=response.url,
                        detail_page=response.urljoin(href)
                    )