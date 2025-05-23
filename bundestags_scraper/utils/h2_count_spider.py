import json
import scrapy
from collections import Counter
from scrapy import signals
from .base_spider import SourceMixin
from bundestags_scraper.items import SourcePageItem, SourceDomainItem

class H2CountSpider(SourceMixin, scrapy.Spider):
    """
    Spider to fetch each politician's detail page, count all <h2> headings,
    and output aggregated counts to 'h2_counts.json' on close.
    """
    name = "h2_count"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Counter for heading texts
        self.heading_counts = Counter()

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(H2CountSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def start_requests(self):
        """
        Read existing PoliticianItem records from 'politicians_output.jsonl'
        and schedule requests to each detail_page URL.
        """
        with open('politicians.jsonl', encoding='utf-8') as f:
            for line in f:
                record = json.loads(line)
                if record.get('item_type') == 'politician':
                    url = record.get('detail_page')
                    if url:
                        yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        """
        Count <h2> headings on a politician detail page.
        Also emit source traceability items.
        """
        # Source items
        #dom_item = self.generate_source_domain_item(response)
        #if dom_item:
        #    yield dom_item
        #yield self.generate_source_page_item(response)

        # Count headings
        for h2 in response.xpath('//h2'):
            text = h2.xpath('normalize-space(string())').get()
            if text:
                self.heading_counts[text] += 1

    def spider_closed(self, spider, reason):
        """
        When spider finishes, write aggregated counts to JSON.
        """
        with open('h2_counts.json', 'w', encoding='utf-8') as f:
            json.dump(self.heading_counts, f, ensure_ascii=False, indent=2)
