import json
import scrapy

# Class and fucntions for scraping the content of the HTML-pages
class WikiHTMLSpider(scrapy.Spider):
    name = "wiki_html"

    def start_requests(self):
        self.minister_data = []
        with open("minister.json", "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
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


# automaton for extracting the needed biography
def extract_bio_sections(minister):
    """
    Extrahiert alle Textabschnitte, die auf das Muster P → Div → P... bis zum nächsten Div passen.
    Gibt eine Liste von Strings zurück, jeweils der zusammengefasste Text.
    """
    sections = []
    buffer = []
    state = 0
    # State-Definitionen:
    # 0 = suche erstes 'p'
    # 1 = suche erstes 'div' nach diesem 'p'
    # 2 = suche erstes 'p' nach dem 'div' (Start der Bio)
    # 3 = sammle alle folgenden 'p' bis zum nächsten 'div'

    for item in minister["wiki_content"]:
        typ = item.get("type")
        text = item.get("text", "").strip()

        if state == 0:
            if typ == "p":
                state = 1
        elif state == 1:
            if typ == "div":
                state = 2
                buffer.append(text)
        elif state == 2:
            if typ == "p":
                buffer.append(text)
                state = 3
        elif state == 3:
            if typ == "p":
                buffer.append(text)
            elif typ == "div":
                # Sobald ein Nicht-'p' kommt ⇒ Abschnitt fertig
                sections.append("\n\n".join(buffer))
                buffer = []
                break

    # Falls Buffer am Ende noch befüllt ist (z.B. kein nachfolgendes Div mehr)
    if buffer:
        sections.append("\n\n".join(buffer))

    return sections


if __name__ == '__main__':
    with open("minister_with_content.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    for minister in data:
        minister["bio_section"] = extract_bio_sections(minister)
    with open("minister_with_content.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)



