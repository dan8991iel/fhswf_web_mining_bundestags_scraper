import logging, hashlib
from itemadapter import ItemAdapter
from neo4j import GraphDatabase, Transaction
from typing import Dict, List
from urllib.parse import urlparse

from bundestags_scraper.items import (
    SourceDomainItem, 
    SourcePageItem, 
    LegislativePeriodItem,
    PoliticianItem, 
    PoliticianContent,
)

_LOG = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
#  Helpers                                                                    #
# --------------------------------------------------------------------------- #

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

def normalize_party_name(raw: str | None) -> str | None:
    if raw is None:
        return None
    cleaned = raw.strip()
    return PARTY_ALIASES.get(cleaned, cleaned)

# --------------------------------------------------------------------------- #
#  Batch helper                                                               #
# --------------------------------------------------------------------------- #

BATCH_SIZE = 500
_BATCHED_TYPES = {"page", "politician", "content"}  # item_type values


class _BatchBuffer:
    """
    Collects items per type and flushes them once BATCH_SIZE is reached.
    """

    def __init__(self, driver):
        self.driver = driver
        self.buf: Dict[str, List[dict]] = {t: [] for t in _BATCHED_TYPES}

    # –– public ---------------------------------------------------------
    def add(self, item_type: str, data: dict):
        if item_type not in self.buf:
            return
        bucket = self.buf[item_type]
        bucket.append(data)
        if len(bucket) >= BATCH_SIZE:
            self._flush_type(item_type)

    def flush_all(self):
        for typ in list(self.buf):
            self._flush_type(typ)

    # –– private --------------------------------------------------------
    def _flush_type(self, item_type: str):
        batch = self.buf[item_type]
        if not batch:
            return
        self.buf[item_type] = []  # clear early → easier error recovery

        def _write(tx: Transaction):
            for row in batch:
                if item_type == "page":
                    Neo4jPipeline._page(tx, row)
                elif item_type == "politician":
                    Neo4jPipeline._politician(tx, row)
                elif item_type == "content":
                    Neo4jPipeline._content(tx, row)

        # one transaction = one network round-trip
        with self.driver.session() as ses:
            ses.execute_write(_write)

# --------------------------------------------------------------------------- #
#  Main pipeline                                                              #
# --------------------------------------------------------------------------- #

class Neo4jPipeline:
    """
    Writes Scrapy items into Neo4j.

    High-volume items (Page, Politician, Content) are buffered and written
    in batches to reduce network I/O.  Low-volume items (Domain, Period)
    are still written immediately.
    """

     # ----------  Scrapy hooks  ----------------------------------------
    def __init__(self, uri: str, user: str, pwd: str):
        self._uri, self._user, self._pwd = uri, user, pwd
        self._driver = None
        self._buffer: _BatchBuffer | None = None

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
        self._buffer = _BatchBuffer(self._driver)

    def close_spider(self, _):
        # flush remaining batched items
        if self._buffer:
            self._buffer.flush_all()
        if self._driver:
            self._driver.close()

    # ----------  Item router  --------------------------------------------
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        data = adapter.asdict()
        item_type = data.get("item_type")

        # high-volume types → buffer
        if item_type in _BATCHED_TYPES and self._buffer:
            self._buffer.add(item_type, data)
            return item

        # everything else → immediate write
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
    def _ensure_page(tx: Transaction, url, title=None, html=None):
        """
        MERGE a Page node + its BELONGS_TO_DOMAIN edge.
        """
        domain = urlparse(url).netloc
        tx.run(
            """
            MERGE (d:Domain {name:$domain})
            MERGE (p:Page {url:$url})
            ON CREATE SET p.title = $title,
                          p.html  = $html
            ON MATCH  SET p.title = coalesce(p.title, $title),
                          p.html  = coalesce(p.html,  $html)
            MERGE (p)-[:BELONGS_TO_DOMAIN]->(d)
            """,
            url=url,
            title=title,
            html=html,
            domain=domain,
        )
        
    # Domain ----------------------------------------------------------------
    @staticmethod
    def _dom(tx: Transaction, d):
        tx.run(
            "MERGE (d:Domain {name:$name}) "
            "ON CREATE SET d.description=$desc",
            name=d["domain"],
            desc=d.get("description"),
        )

    # Page ------------------------------------------------------------------
    @staticmethod
    def _page(tx: Transaction, p):
        tx.run(
            """
            MERGE (pg:Page {url:$url})
            ON CREATE SET pg.title = $title,
                          pg.html  = $html
            ON MATCH  SET pg.title = coalesce(pg.title, $title),
                          pg.html  = coalesce(pg.html,  $html)
            WITH pg
            MATCH (d:Domain {name:$dom})
            MERGE (pg)-[:BELONGS_TO_DOMAIN]->(d)
            """,
            url=p["url"],
            title=p.get("title"),
            html=p.get("full_html"),
            dom=p["source_domain"],
        )

    @staticmethod
    def _merge_page(tx: Transaction, p):
        Neo4jPipeline._ensure_page(
            tx, url=p["url"], title=p.get("title"), html=p.get("full_html")
        )
    
    # Period ----------------------------------------------------------------
    @staticmethod
    def _period(tx: Transaction, pr):
        tx.run(
            """
            MERGE (per:Period {number:$nr})
            ON CREATE SET per.name       = $name,
                          per.start_date = $st,
                          per.end_date   = $end
            """,
            nr=pr["period_number"],
            name=pr["name"],
            st=pr["start_date"],
            end=pr["end_date"],
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
                src=pr["source_page"],
                nr=pr["period_number"],
            )
            
        # link to detail list page
        if pr.get("detail_page"):
            Neo4jPipeline._ensure_page(tx, url=pr["detail_page"])
            tx.run(
                """
                MATCH (per:Period {number:$nr})
                MATCH (det:Page {url:$url})
                MERGE (per)-[:HAS_DETAIL_PAGE]->(det)
                """,
                nr=pr["period_number"],
                url=pr["detail_page"],
            )

    # Politician ------------------------------------------------------------
    @staticmethod
    def _politician(tx: Transaction, pol):
        src, det = pol["source_page"], pol["detail_page"]
        normalized_party = normalize_party_name(pol.get("political_party"))
        
        
        # core node ----------------------------------------------------
        tx.run(
            """
            MERGE (po:Politician {detail_page:$url})
            ON CREATE SET po.full_name  = $full,
                          po.firstname  = $first,
                          po.lastname   = $last,
                          po.birth_year = $birth,
                          po.death_year = $death
            ON MATCH  SET po.full_name  = coalesce(po.full_name, $full)
            """,
            url=det,
            full=pol["full_name"],
            first=pol.get("firstname"),
            last=pol.get("lastname"),
            birth=pol.get("birth_year"),
            death=pol.get("death_year"),
        )

   
        # pages --------------------------------------------------------
        if src:
            Neo4jPipeline._ensure_page(tx, src)
            tx.run(
                """
                MATCH (po:Politician {detail_page:$det})
                MATCH (pg:Page {url:$src})
                MERGE (po)-[:HAS_SOURCE_PAGE]->(pg)
                """,
                det=det,
                src=src,
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
                src=src,
                det=det,
            )
            
            
        # period edge --------------------------------------------------
        if pol.get("legislative_period_number"):
            tx.run(
                """
                MATCH (po:Politician {detail_page:$det})
                MATCH (per:Period {number:$nr})
                MERGE (po)-[:SERVED_DURING]->(per)
                """,
                det=det,
                nr=pol["legislative_period_number"],
            )

        # ── Mandate (one per politician & period combination) ────────────────
        # mandate node ---------------------------------------------
            mandate_id = _mandate_id(pol["legislative_period_number"], det)
            tx.run(
                """
                MERGE (m:Mandate {id:$mid})
                ON CREATE SET m.political_party = $party,
                              m.federate_state  = $state,
                              m.constituency    = $const
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
                det=det,
                nr=pol["legislative_period_number"],
                mid=mandate_id,
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
                    party=normalized_party,
                    mid=mandate_id,
                )
            if pol.get("federate_state"):
                tx.run(
                    """
                    MERGE (st:State {name:$state})
                    WITH st
                    MATCH (m:Mandate {id:$mid})
                    MERGE (m)-[:REPRESENTS_STATE]->(st)
                    """,
                    state=pol["federate_state"],
                    mid=mandate_id,
                )
            if pol.get("constituency"):
                tx.run(
                    """
                    MERGE (co:Constituency {name:$const})
                    WITH co
                    MATCH (m:Mandate {id:$mid})
                    MERGE (m)-[:REPRESENTS_CONSTITUENCY]->(co)
                    """,
                    const=pol["constituency"],
                    mid=mandate_id,
                )
            

    # Content ---------------------------------------------------------------
    @staticmethod
    def _content(tx: Transaction, pc):
        cid = hashlib.sha1(
            f"{pc['source_page']}#{pc['section_header']}".encode("utf-8")
        ).hexdigest()

        Neo4jPipeline._ensure_page(tx, url=pc["source_page"])

        tx.run(
            """
            MERGE (c:Content {id:$cid})
            ON CREATE SET c.section_header  = $hdr,
                          c.section_content = $txt
            ON MATCH  SET c.section_content = coalesce(c.section_content, $txt)
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
