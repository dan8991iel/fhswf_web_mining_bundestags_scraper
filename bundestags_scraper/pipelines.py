import json, logging, hashlib
from itemadapter import ItemAdapter
from neo4j import GraphDatabase
from urllib.parse import urlparse

from bundestags_scraper.items import (
    SourceDomainItem, SourcePageItem, LegislativePeriodItem,
    PoliticianItem, PoliticianContent,
)

_LOG = logging.getLogger(__name__)


def _mandate_id(period_nr: str, pol_url: str) -> str:
    data = f"{period_nr}#{pol_url}".encode("utf-8")
    return hashlib.sha1(data).hexdigest()

PARTY_ALIASES = {
    # CDU / CSU
    "CDU/CSU (CDU)": "CDU",
    "CSU (GDP)":     "CSU",
    "CDU/CSU (CSU)": "CSU",

    # SPD
    "SPD (GDP)":     "SPD",

    # Bündnis 90/Die Grünen
    "Die Grünen":  "Bündnis 90/Die Grünen",
    "GRÜNE":       "Bündnis 90/Die Grünen",
    "Grüne":       "Bündnis 90/Die Grünen",
    "Bündnis 90":  "Bündnis 90/Die Grünen",
    "Grüne DDR":   "Bündnis 90/Die Grünen",

    # AfD
    "AfD (parteilos)":   "AfD",
    "fraktionslos(AfD)": "AfD",

    # DIE LINKE
    "Die Linke": "DIE LINKE",
    "Linke":     "DIE LINKE",
    "PDS":       "DIE LINKE",

    # Unabhängig / Parteilos
    "parteilos":                  "Unabhängig / Parteilos",
    "unabhängig":                 "Unabhängig / Parteilos",
    "fraktionslos":               "Unabhängig / Parteilos",
    "fraktionslos (Die PARTEI)":  "Unabhängig / Parteilos",
    "fraktionslos (LKR)":         "Unabhängig / Parteilos",
    "fraktionslos(SSW)":          "Unabhängig / Parteilos",

    # BSW
    "BSW": "BSW",
}

def normalize_party_name(raw: str) -> str:
    cleaned = raw.strip() if raw else ""
    return PARTY_ALIASES.get(cleaned, cleaned)



class Neo4jPipeline:
    """Writes every item into Neo4j with :SOURCE_PAGE edges."""

    def __init__(self, uri, user, pwd):
        self._uri, self._user, self._pwd = uri, user, pwd
        self._driver = None

    # ----------  Scrapy hooks  --------------------------------------------
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            crawler.settings["NEO4J_URI"],
            crawler.settings["NEO4J_USER"],
            crawler.settings["NEO4J_PASSWORD"],
        )

    def open_spider(self, _):
        _LOG.info("[Neo4jPipeline] connect → %s", self._uri)
        self._driver = GraphDatabase.driver(self._uri, auth=(self._user, self._pwd))

    def close_spider(self, _):
        if self._driver:
            self._driver.close()

    # ----------  Item router  --------------------------------------------
    def process_item(self, item, spider):
        data = ItemAdapter(item).asdict()
        with self._driver.session() as ses:
            if isinstance(item, SourceDomainItem):
                ses.write_transaction(self._dom, data)
            elif isinstance(item, SourcePageItem):
                ses.write_transaction(self._page, data)
            elif isinstance(item, LegislativePeriodItem):
                ses.write_transaction(self._period, data)
            elif isinstance(item, PoliticianItem):
                ses.write_transaction(self._politician, data)
            elif isinstance(item, PoliticianContent):
                ses.write_transaction(self._content, data)
        return item

    # ----------  Cypher helpers  -----------------------------------------
    
    
    
    
    # ───────────────── page helper ──────────────────────────────────
    @staticmethod
    def _ensure_page(tx, url, title = None, html = None):
        """
        Always creates/updates a Page node AND its BELONGS_TO_DOMAIN edge.
        Returns nothing (Cypher MERGE is idempotent).
        """
        domain = urlparse(url).netloc
        tx.run(
            """
            // ensure domain
            MERGE (d:Domain {name:$domain})
            // ensure page + edge
            MERGE (p:Page {url:$url})
            ON CREATE SET p.title=$title, p.html=$html
            ON MATCH  SET p.title = coalesce(p.title,$title)
            MERGE (p)-[:BELONGS_TO_DOMAIN]->(d)
            """,
            url=url, title=title, html=html, domain=domain,
        )
        
    # Domain ----------------------------------------------------------------
    @staticmethod
    def _dom(tx, d):
        tx.run(
            "MERGE (d:Domain {name:$name}) "
            "ON CREATE SET d.description=$desc",
            name=d["domain"], desc=d.get("description")
        )

    # Page ------------------------------------------------------------------
    @staticmethod
    def _page(tx, p):
        tx.run(
            """
            MERGE (pg:Page {url:$url})
            ON CREATE SET pg.title=$title, pg.html=$html
            ON MATCH  SET pg.title = coalesce(pg.title,$title), pg.html  = coalesce(pg.html,$html)
            WITH pg
            MATCH (d:Domain {name:$dom})
            MERGE (pg)-[:BELONGS_TO_DOMAIN]->(d)
            """,
            url=p["url"], title=p.get("title"), html=p.get("full_html"),
            dom=p["source_domain"]
        )

    @staticmethod
    def _merge_page(tx, p):
        Neo4jPipeline._ensure_page(
            tx, url=p["url"], title=p.get("title"), html=p.get("full_html")
        )
    
    # Period ----------------------------------------------------------------
    @staticmethod
    def _period(tx, pr):
        # node
        tx.run(
            "MERGE (per:Period {number:$nr}) "
            "ON CREATE SET per.name=$name, per.start_date=$st, per.end_date=$end",
            nr=pr["period_number"], name=pr["name"],
            st=pr["start_date"], end=pr["end_date"]
        )
        # source page (list-of-lists)
        if pr.get("source_page"):
            Neo4jPipeline._ensure_page(tx, url=pr["source_page"])
            tx.run(
                """
                MERGE (pg:Page {url:$src})
                WITH pg
                MATCH (per:Period {number:$nr})
                MERGE (per)-[:HAS_SOURCE_PAGE]->(pg)
                """,
                src=pr["source_page"], nr=pr["period_number"]
            )
        # detail page (member list) ⇒ just keep the Page node & link pages
        if pr.get("detail_page"):
            Neo4jPipeline._ensure_page(tx, url=pr["detail_page"])
            tx.run(
                """
                MATCH (per:Period {number:$nr})
                MATCH (det:Page {url:$url})
                MERGE (per)-[:HAS_DETAIL_PAGE]->(det)
                """,
                nr=pr["period_number"], url=pr["detail_page"],
            )

    # Politician ------------------------------------------------------------
    @staticmethod
    def _politician(tx, pol):
        src, det = pol["source_page"], pol["detail_page"]
        raw_party = pol["political_party"]
        
        normalized_party = normalize_party_name(raw_party) if raw_party else None
        
        
        # core node -----------------------------------------------------
        tx.run(
            """
            MERGE (po:Politician {detail_page:$url})
            ON CREATE SET po.full_name=$full, po.firstname=$first,
                          po.lastname=$last, po.birth_year=$birth, po.death_year=$death
            ON MATCH  SET po.full_name=coalesce(po.full_name,$full)
            """,
            url=det, full=pol["full_name"], first=pol.get("firstname"),
            last=pol.get("lastname"), birth=pol.get("birth_year"),
            death=pol.get("death_year")
        )

        # ── Pages and direct links ─────────────────────────────────
        
        if src:
            Neo4jPipeline._ensure_page(tx, src)
            tx.run(
                """
                MATCH (po:Politician {detail_page:$det})
                MATCH (pg:Page {url:$src})
                MERGE (po)-[:HAS_SOURCE_PAGE]->(pg)
                """,
                det=det, src=src,
            )
        Neo4jPipeline._ensure_page(tx, det)
        tx.run(
            """
            MATCH (po:Politician {detail_page:$det})
            MATCH (d:Page {url:$det})
            MERGE (po)-[:HAS_DETAIL_PAGE]->(d)
            """,
            det=det,
        )
        if src:
            tx.run(
                """
                MATCH (l:Page {url:$src})
                MATCH (d:Page {url:$det})
                MERGE (l)-[:LINKS_TO_DETAIL]->(d)
                """,
                src=src, det=det,
            )

        # ── Direct convenience edge ───────────────────────────────
        if pol.get("legislative_period_number"):
            tx.run(
                """
                MATCH (po:Politician {detail_page:$det})
                MATCH (per:Period {number:$nr})
                MERGE (po)-[:SERVED_DURING]->(per)
                """,
                det=det, nr=pol["legislative_period_number"],
            )

        # ── Mandate node (one per politician & period combination) ────────────────
        if pol.get("legislative_period_number"):
            mandate_id = _mandate_id(pol["legislative_period_number"], det)
            tx.run(
                """
                MERGE (m:Mandate {id:$mid})
                ON CREATE SET m.political_party=$party,
                              m.federate_state=$state,
                              m.constituency=$const
                ON MATCH  SET m.political_party = coalesce(m.political_party,$party),
                              m.federate_state  = coalesce(m.federate_state,$state),
                              m.constituency    = coalesce(m.constituency,$const)
                """,
                mid=mandate_id,
                party=normalized_party,
                state=pol.get("federate_state"),
                const=pol.get("constituency"),
            )
            # connect Mandate
            tx.run(
                """
                MATCH (po:Politician {detail_page:$det})
                MATCH (per:Period {number:$nr})
                MATCH (m:Mandate {id:$mid})
                MERGE (po)-[:HAS_MANDATE]->(m)
                MERGE (m)-[:IN_PERIOD]->(per)
                """,
                det=det, nr=pol["legislative_period_number"], mid=mandate_id,
            )

            # Party / State / Constituency edges (optional) --------
            if pol.get("political_party"):
                tx.run(
                    """
                    MERGE (pa:Party {name:$party})
                    WITH pa
                    MATCH (m:Mandate {id:$mid})
                    MERGE (m)-[:AFFILIATED_WITH]->(pa)
                    """,
                    party=normalized_party, mid=mandate_id,
                )
            if pol.get("federate_state"):
                tx.run(
                    """
                    MERGE (st:State {name:$state})
                    WITH st
                    MATCH (m:Mandate {id:$mid})
                    MERGE (m)-[:REPRESENTS_STATE]->(st)
                    """,
                    state=pol["federate_state"], mid=mandate_id,
                )
            if pol.get("constituency"):
                tx.run(
                    """
                    MERGE (co:Constituency {name:$const})
                    WITH co
                    MATCH (m:Mandate {id:$mid})
                    MERGE (m)-[:REPRESENTS_CONSTITUENCY]->(co)
                    """,
                    const=pol["constituency"], mid=mandate_id,
                )
            

    # Content ---------------------------------------------------------------
    @staticmethod
    def _content(tx, pc):
        cid = hashlib.sha1(
            f"{pc['source_page']}#{pc['section_header']}".encode("utf-8")
        ).hexdigest()
         
        Neo4jPipeline._ensure_page(tx, url=pc["source_page"])
        
         
        tx.run(
            """
            MERGE (c:Content {id:$cid})
            ON CREATE SET c.section_header  = $hdr,
                          c.section_content = $txt
            ON MATCH  SET c.section_content = coalesce(c.section_content,$txt)
            """,
            cid=cid,
            hdr=pc["section_header"],
            txt=pc["section_content"],
        )
        
        tx.run(
            """
            MATCH (c:Content {id:$cid})
            MATCH (pg:Page {url:$url})
            MERGE (c)-[:HAS_SOURCE_PAGE]->(pg)
            """,
            cid=cid,
            url=pc["source_page"],
        )
        
        tx.run(
            """
            MATCH (po:Politician {detail_page:$url})
            MATCH (c:Content {id:$cid})
            MERGE (po)-[:HAS_CONTENT]->(c)
            """,
            url=pc["source_page"],
            cid=cid,
        )
