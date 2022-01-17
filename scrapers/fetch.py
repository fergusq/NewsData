import json
import logging
from typing import List, NamedTuple, Optional, Tuple
from bs4 import BeautifulSoup
import re
import aiohttp

logger = logging.getLogger("news_fetch")

class FetchSelectors(NamedTuple):
    url_regex: str
    article: str
    persons: str

SELECTORS: List[FetchSelectors] = [
    FetchSelectors(r"https?://yle.fi/.*", 
        article=".yle__article__heading--h1, .yle__article__paragraph",
        persons=".yle__article__quote__source, .yle__article__strong:not(:first-child:last-child)"
    ),
    FetchSelectors(r"https://www.is.fi/.*", 
        article=".article-title-40, .article-ingress-20, p.article-body",
        persons=".article-personlink"
    ),
    FetchSelectors(r"https://iltalehti.fi/.*", 
        article=".article-headline, .article-description, .article-body .paragraph",
        persons="p.paragraph strong"
    ),
]

class FetchResult(NamedTuple):
    content: str
    persons: List[str]

    @staticmethod
    def from_json(json_code: str) -> "FetchResult":
        obj = json.loads(json_code)
        return FetchResult(
            content=obj.get("content", ""),
            persons=obj.get("persons", [])
        )

    def to_json(self):
        return json.dumps({
            "content": self.content,
            "persons": self.persons,
        })

async def css_fetch(url: str, session: aiohttp.ClientSession) -> Optional[FetchResult]:
    logger.info(f"Fetching {url}")
    async with session.get(url) as response:
        if response.status != 200:
            logger.error(f"Got unexpected response code {response.status} for {response.url}.")
            return None
        
        s = BeautifulSoup(await response.text(), "lxml")

        article = ""
        persons = []
        for site in SELECTORS:
            if re.fullmatch(site.url_regex, url):
                texts = s.select(site.article)
                if texts:
                    for text in texts:
                        article += text.get_text() + "\n"
                
                texts = s.select(site.persons)
                if texts:
                    for text in texts:
                        persons.append(text.get_text())
                
                break
        
        else:
            logger.error(f"No known CSS selectors for {response.url}.")
        
        return FetchResult(content=article, persons=persons)