import asyncio
import contextlib
import re
import sys
from datetime import date, datetime
from typing import Dict, List, Optional

import pyppeteer
from bs4 import BeautifulSoup
from bs4.element import NavigableString
from pyppeteer.browser import Browser
from pyppeteer.element_handle import ElementHandle
from pyppeteer.page import Page
import pyppdf.patch_pyppeteer

from scrapers.fetch import FetchResult, logger as fetch_logger
from scrapers.query import PaginatedQuery, Params, QueryResult, logger as query_logger

class HSQuery(PaginatedQuery):
    MAX_LIMIT = 100
    BASE_URL = "https://www.hs.fi"
    API_URL = "https://www.hs.fi/api/search"

    def build_url(self) -> str:
        date_start = int(datetime.timestamp(datetime.combine(self.params.from_date, datetime.min.time())) * 1000)
        date_end = int(datetime.timestamp(datetime.combine(self.params.to_date if self.params.to_date is not None else date.today(), datetime.max.time())) * 1000)
        return f"{self.API_URL}/{self.params.query}/kaikki/custom/new/{self.offset}/{self.params.limit}/{date_start}/{date_end}"
    
    def parse_response(self, r):
        if self.offset >= 9900:
            query_logger.error(f"Query results in more than 9900 results. The Sanoma API refuses to return more than 10000 results, so some results are missing. You can work around this limitation by doing multiple queries on smaller timespans.")
            
        ans: List[QueryResult] = []
        for a in r:
            if "nakoislehti.hs.fi" in a["href"]:
                continue # skipataan näköislehtiartikkelit, koska niitä ei osata parsia
            
            ans.append({
                "url": a["href"] if a["href"].startswith("http") else self.BASE_URL+a["href"],
                "title": a["title"],
                "date_modified": a["displayDate"],
                "lead": a["ingress"] if "ingress" in a else "",
                "id": a["id"],
            })
        
        return ans

class ISQuery(HSQuery):
    BASE_URL = "https://www.is.fi"
    API_URL = "https://www.is.fi/api/search"

@contextlib.asynccontextmanager
async def create_hs_session(username: str, password: str):
    hs_fetch = HSFetch(username, password)
    await hs_fetch.login()
    yield hs_fetch
    await hs_fetch.close()

class HSFetch:
    browser: Optional[Browser]
    page: Optional[Page]

    def __init__(self, username: str, password: str):
        self.browser = None
        self.page = None
        self.username = username
        self.password = password
    
    async def login(self):
        if not self.browser:
            self.browser = await pyppeteer.launch(headless=True, args=["--no-sandbox", "--disable-gpu"])

        page = await self.browser.newPage()
        fetch_logger.info("Logging into HS.")
        await page.goto("https://www.hs.fi")
        try:
            consent_form = await page.waitForXPath("//iframe[@title='SP Consent Message']")
            consent_frame = await consent_form.contentFrame()
            ok = await consent_frame.waitForXPath("//button[@title='OK']")
            await ok.click()
        
        except:
            fetch_logger.error("Failed to close cookie consent form", exc_info=sys.exc_info())

        await page.waitForSelector("a[href*=start-login]")
        await page.querySelectorEval("a[href*=start-login]", "a => a.click()")
        await page.screenshot(path="hsfi.png")
        user = await page.waitForSelector("#username")
        await user.type(self.username)
        pas = await page.querySelector("#password")
        await pas.type(self.password),
        submit = await page.querySelector("button[type=submit]")
        await submit.click()
        await asyncio.sleep(10)
        fetch_logger.info("Logged in.")

        self.page = page
    
    async def close(self):
        if self.browser:
            await self.browser.close()

    async def fetch_hs(self, url: str) -> Optional[FetchResult]:
        fetch_logger.info(f"Fetching {url}")
        try:
            await self.page.goto(url)
            dynamic_content: ElementHandle = await self.page.waitForXPath("//div[@id='page-main-content']/following-sibling::*")
            if await (await dynamic_content.getProperty("tagName")).jsonValue() == "IFRAME":
                frame = await dynamic_content.contentFrame()
                await frame.waitForXPath("//div[@class='paywall-content']|//div[@id='paid-content']")
                content = await frame.content()
            else:
                content = await self.page.content()
            
            return self._parse_hs(content)
        except:
            fetch_logger.exception(f"Failed to fetch {url}.", exc_info=sys.exc_info())
            return None

    def _parse_hs(self, html: str) -> FetchResult:
        soup = BeautifulSoup(html, "lxml")

        persons = soup.select(".article-personlink")
        persons = [] if not persons else [p.get_text() for p in persons]

        soup = soup.find("main") or soup
        soup = soup.select_one("div#page-main-content + article") or soup.select_one("div#page-main-content") or soup
        for elem in soup.find_all("aside"):
            elem.extract()
        
        for elem in soup.select("section.article-body + div, div.article-info, div.related-articles, div.article-actions, div.paywallWrapper"):
            elem.extract()
        
        for tag in ["h1", "h2", "h3", "h4", "h5", "h6", "h7", "p", "div"]:
            for p in soup.find_all(tag):
                p.insert_after(NavigableString("\n\n"))
        
        text: str = soup.get_text().replace("\xad", "")
        text = re.sub("\n\n+", "\n\n", text)

        return FetchResult(content=text, persons=persons)
