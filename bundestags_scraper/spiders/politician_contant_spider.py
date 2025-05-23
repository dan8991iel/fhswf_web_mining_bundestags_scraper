import json
import logging
import unicodedata, html, re
import scrapy
from bundestags_scraper.items import PoliticianContent
from .base_spider import LoggingMixin, SourceMixin, Neo4jMixin

class PoliticianContentSpider(LoggingMixin, SourceMixin, Neo4jMixin, scrapy.Spider):
    """
    Scrape politician content from a list of politician items.

    Reads a list of politicians from 'politicans.jsonl',
    parses the list and saves for each a key value map of h2 element and content.
    """
    name = "politician_content_spider"
    BATCH_SIZE = 2000 

    def __init__(self, batch: int = 0, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.batch = int(batch) 

    def start_requests(self):
        skip  = self.batch * self.BATCH_SIZE
        limit = self.BATCH_SIZE
        
        """
        Read 'politician' entries from politicans.jsonl and generate requests.
        """
        self.log_event(
            "debug","Querying Neo4j for politician detail pages",
            batch=self.batch, skip=skip, limit=limit,
        )
        query = (
            "MATCH (po:Politician) "
            "WHERE po.detail_page IS NOT NULL "
            "RETURN po.detail_page AS url "
            "ORDER BY url "
            "SKIP  $skip "
            "LIMIT $limit"
        )
        with self.neo4j_session() as session:
            for rec in session.run(query, skip=skip, limit=limit):
                self.log_event("debug", "queue_detail_page", url=rec["url"])
                yield scrapy.Request(rec["url"], callback=self.parse)

    def parse(self, response):
        '''yield self.generate_source_page_item(response)'''
        self.log_event("info", "start_parse_content", url=response.url)
        
        for src in self.add_source_page(response):
            yield src
        
        
        container = response.xpath(
            "//div[@id='mw-content-text']/div[contains(@class,'mw-parser-output')]"
        )
        if not container:
            self.log_event("warning", "no_content_container", url=response.url)
            return
        children = container.xpath('./*')
        if not children:
            self.log_event("warning", "no_content_container_children", url=response.url)
            return

        sections = {}
        current = '#'
        sections[current] = []

        for elem in children:
            if elem.xpath("@id").get() == 'normdaten':
                # self.logger.debug(f"Reached 'normdaten' at {response.url}, ending parse loop.")
                break
            if elem.root.tag == 'div' and elem.xpath('.//h2'):
                title = elem.xpath('.//h2//text()').get(default='').strip()
                if title:
                    current = title
                    sections[current] = []
                continue
            text = elem.xpath('string()').get()
            if text:
                cleaned = ' '.join(text.split())
                sections[current].append(cleaned)

        
        for key in list(sections.keys()):
            sections[key] = '\n'.join(sections[key]).strip()
            #if not sections[key]:
                #self.logger.debug(f"Section '{key}' on {response.url} is empty after joining.")


        self.log_event("debug", "yield_politician_content_item", url=response.url)
        
        for key in sections:
            yield PoliticianContent(
                item_type='politician_content',
                source_page= response.url,
                section_header = key,
                section_content = sections[key]
            )