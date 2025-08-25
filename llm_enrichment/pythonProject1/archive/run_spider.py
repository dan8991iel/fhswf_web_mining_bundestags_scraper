# run_spider.py
from scrapy.crawler import CrawlerProcess
from scrapy.utils.log import configure_logging

# importiere hier deinen Spider
from scraping import WikiHTMLSpider

if __name__ == "__main__":
    # Logging auf INFO setzen (optional)
    configure_logging({"LOG_LEVEL": "INFO"})

    process = CrawlerProcess(settings={
        # kein Zugriff auf irgendwelche project settings!
        "NO_PROJECT_SETTINGS": True,
        # definiere Output-File und Format
        "FEEDS": {
            "minister_with_content.json": {
                "format": "json",
                "encoding": "utf-8",
                "overwrite": True,
            },
        },
    })

    process.crawl(WikiHTMLSpider)
    process.start()