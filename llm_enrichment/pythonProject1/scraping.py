import json
import scrapy


class WikiHTMLSpider(scrapy.Spider):
    name = "wiki_html"

    def start_requests(self):
        # Die ersten 10 Zeilen/Objekte aus der JSON laden
        self.minister_data = []
        with open("minister.json", "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i >= 20:
                    break
                data = json.loads(line)
                self.minister_data.append(data)

        # Für jedes Objekt einen Request erzeugen, das Objekt per meta mitgeben
        for minister in self.minister_data:
            yield scrapy.Request(
                url=minister["Link"],
                callback=self.parse,
                meta={"minister": minister}
            )

    def parse(self, response):
        minister = response.meta["minister"]
        content = []
        for elem in response.css('div.mw-parser-output > *'):
            tag = elem.root.tag
            text = elem.xpath('normalize-space(string())').get()
            if text.strip():
                content.append({"type": tag, "text": text})

        minister["wiki_content"] = content
        yield minister






'''
    def parse(self, response):
        minister = response.meta["minister"]
        content = []
        for elem in response.css('div.mw-parser-output > *'):
            tag = elem.root.tag
            print(tag)
            if tag == 'h2':
                heading = elem.xpath('normalize-space(string())').get()
                content.append({"type": "heading", "text": heading})
            elif tag == 'p':
                para = elem.xpath('normalize-space(string())').get()
                if para.strip():
                    content.append({"type": "paragraph", "text": para})
        minister["structured_bio"] = content
        yield minister
'''


        #paragraphs = "".join(response.css('div.mw-parser-output > p').xpath('string()').getall()).strip()
        #minister["bio_text"] = paragraphs