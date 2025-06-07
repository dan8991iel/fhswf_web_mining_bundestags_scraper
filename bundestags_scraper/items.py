# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class BaseItem(scrapy.Item):
    item_type = scrapy.Field();

class PoliticianBiography(BaseItem):
    full_text = scrapy.Field()
    

class PoliticianContent(BaseItem):
    source_page = scrapy.Field()
    content = scrapy.Field()
    section_header = scrapy.Field()
    section_content = scrapy.Field()

    

class PoliticianItem(BaseItem):
    legislative_period_number = scrapy.Field()
    full_name = scrapy.Field()
    firstname = scrapy.Field()
    lastname = scrapy.Field()
    birthname = scrapy.Field()
    birth_year = scrapy.Field()
    death_year = scrapy.Field()
    
    political_party = scrapy.Field()
    federate_state = scrapy.Field()
    constituency = scrapy.Field()
    remarks = scrapy.Field()
    source_page = scrapy.Field()
    detail_page = scrapy.Field()

class LegislativePeriodItem(BaseItem):
    period_number = scrapy.Field()
    name = scrapy.Field()
    start_date = scrapy.Field()
    end_date = scrapy.Field()
    source_page = scrapy.Field()
    detail_page = scrapy.Field()
    
class SourcePageItem(BaseItem):
    url = scrapy.Field()
    title = scrapy.Field()
    full_html = scrapy.Field()
    source_domain = scrapy.Field()
    
class SourceDomainItem(BaseItem):
    domain = scrapy.Field()
    description = scrapy.Field()