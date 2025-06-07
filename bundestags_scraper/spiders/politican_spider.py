import json
import logging
import re
import scrapy
from bundestags_scraper.items import PoliticianItem
from .base_spider import LoggingMixin, SourceMixin, Neo4jMixin

class PoliticianSpider(LoggingMixin, SourceMixin, Neo4jMixin, scrapy.Spider):
    """
    Scrape politician data from Bundestag member list pages.

    Reads legislative period URLs from 'output.jsonl',
    parses the member table after the 'Abgeordnete'
    section into PoliticianItem instances.
    """
    name = "politician_spider"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def start_requests(self):
        self.log_event("info", "load_period_urls")
        query = """
            MATCH (per:Period)-[:HAS_DETAIL_PAGE]->(pg:Page)
            RETURN per.number AS period, pg.url AS url
        """
        with self.neo4j_session() as session:
            for rec in session.run(query):
                self.log_event("debug", "queue_period", period=rec["period"], url=rec["url"])
                yield scrapy.Request(
                    rec['url'],
                    callback=self.parse,
                    meta={'period_number': rec['period']}
                )

    def parse(self, response):
        """
        Parse a membership list page.

        1. Yield SourceDomainItem and SourcePageItem.
        2. Extract and map table headers.
        3. Parse each row into a PoliticianItem.
        """
        self.log_event("info", "start_parse_politicians", url=response.url)
        
        for src in self.add_source_page(response):
            yield src
        
        table = response.xpath(
            "//h2[@id='Abgeordnete']/../following::table[1]"
        )
        if not table:
            self.log_event("warning", "no_table_found", url=response.url)
            return

        headers = self.extract_headers(table)
        header_map = self.map_headers(headers)

        rows = table.xpath(".//tbody/tr")
        
        self.log_event("info", "found_rows", count=len(rows), url=response.url)
        
        
        mandatory = [
            'full_name', 'firstname', 'lastname',
            'birth_year', 'detail_page', 'legislative_period_number'
        ]
                         
        for row in rows:
            item = self.parse_row(row, header_map, response)
            self.validate_item(item, mandatory, response.url)
            self.log_event("debug", "yield_politician_item",name=item.get("full_name"),detail_page=item.get("detail_page"))
            yield item

    def extract_headers(self, table):
        """
        Extract and clean header texts from a table.

        Strips small/sup tags, handles <br> as space, and normalizes whitespace.

        :param table: Source table.
        :return: List of cleaned header strings.
        """
        headers = []
        raw_headers = table.xpath(".//th")
        
        for th in raw_headers:
            header_parts = th.xpath(".//text()[not(ancestor::small)] | .//br").getall()
            cleaned_parts = []
            for part in header_parts:
                if part == '<br>':
                    if cleaned_parts:
                        cleaned_parts[-1] = cleaned_parts[-1].rstrip().rstrip('-')
                else:
                    if cleaned_parts and cleaned_parts[-1] == '<br>':
                        part = part.lstrip()
                    cleaned_parts.append(part)
            header = ''.join(cleaned_parts)
            header = re.sub(r'[\u00ad]', '', header)
            header = re.sub(r'[\xa0]', ' ', header)
            header = re.sub(r'\s+', ' ', header).strip()
            headers.append(header)
        return headers

    def map_headers(self, headers):
        """
        Map header labels to field keys based on keyword matching.

        :param headers: List of header strings.
        :return: Dict mapping field names to column indices.
        """
        header_rules = {
            'full_name': ['mitglied', 'name'],
            'lifespan': ['lebens', 'geburt'],
            'political_party': ['partei', 'fraktion'],
            'federate_state': ['land'],
            'constituency': ['wahlkreis'],
            'remarks' : ['bemerkung']
        }
        header_mapping = {}
        for idx, header in enumerate(headers):
            header_lower = header.lower()
            
            if 'wahlkreisnr' in header_lower:
                continue
            
            for field, keywords in header_rules.items():
                if any(keyword in header_lower for keyword in keywords):
                    header_mapping[field] = idx
                    break
        return header_mapping

    def parse_row(self, row, header_map, response):
        """
        Parse a table row (<tr>) into a PoliticianItem.

        :param row: Selector for the table row.
        :param header_map: Dict mapping fields to column indices.
        :param response: Scrapy Response object.
        :return: PoliticianItem populated with row data.
        """
        item = PoliticianItem()
        cells = row.xpath('./td')
        def cell(idx): 
            if idx is None:
                return None
            return cells[idx] if idx < len(cells) else None

        idx = header_map.get('full_name')
        if idx is not None:
            self._extract_name(item, cell(idx), response)

        idx = header_map.get('lifespan')
        if idx is not None:
            self._extract_lifespan(item, cell(idx))
        

        for field in ('political_party', 'federate_state', 'constituency', 'remarks'):
            idx = header_map.get(field)
            c = cell(idx)
            if c:
                item[field] = c.xpath('normalize-space(.)').get()

        item.update({
            'item_type': 'politician',
            'source_page': response.url,
            'legislative_period_number': response.meta.get('period_number')
        })
        return item

    def _extract_name(self, item, cell, response):
        """
        Extract full_name, detail_page, firstname, and lastname from a cell.

        :param item: PoliticianItem to populate.
        :param cell: Selector for the cell containing name.
        :param response: Scrapy Response object.
        """
        if not cell:
            return
        a_tag = cell.xpath('.//a')
        full_name = a_tag.xpath('normalize-space(text())').get()
        href = a_tag.xpath('@href').get()
        item['full_name'] = full_name
        item['detail_page'] = response.urljoin(href) if href else None
        sort_value_raw = cell.attrib.get('data-sort-value', '')
        sort_value = sort_value_raw.split('@')[0]
        
        if sort_value and ',' in sort_value:
            item['lastname'], item['firstname'] = map(str.strip, sort_value.split(',', 1))

    def _extract_lifespan(self, item, cell):
        """
        Extract birth_year and optional death_year from a cell.

        :param item: PoliticianItem to populate.
        :param cell: Selector for the lifespan cell.
        """
        if not cell:
            return
        lifespan_text = cell.xpath('normalize-space(.)').get()
        if not lifespan_text:
            return
        years = lifespan_text.replace('–', '-').split('-')
        item['birth_year'] = years[0].strip()
        if len(years) > 1:
            item['death_year'] = years[1].strip()