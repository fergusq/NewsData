import asyncio
import datetime
import aiohttp
import logging
import random
from time import sleep
import pandas

from typing import NamedTuple, Optional, TypedDict, List

logger = logging.getLogger("news_query")

class QueryResult(TypedDict):
    id: str
    url: str
    title: str
    date_modified: str
    lead: str

class Params(NamedTuple):
    query: str
    from_date: datetime.date
    to_date: datetime.date
    limit: int = 0
    delay: float = 1.0
    enabled: List[str] = []
    extra: dict = {}

DATE_DELTA = datetime.timedelta(weeks=1)

class PaginatedQuery:
    MAX_LIMIT = 100
    offset: int = 0

    def __init__(self, params: Params):
        if params.limit == 0:
            params = params._replace(limit = self.MAX_LIMIT)

        self.params = params
    
    def build_url(self) -> str:
        raise NotImplementedError
    
    def build_params(self) -> Optional[dict]:
        return None

    def parse_response(self, response) -> List[QueryResult]:
        raise NotImplementedError
    
    async def _scrape_page(self, session: aiohttp.ClientSession) -> List[QueryResult]:
        url = self.build_url()
        params = self.build_params()
        async with session.get(url, params=params) as response:
            logger.info(f"Processing articles {self.params.from_date.isoformat()}..{self.params.to_date.isoformat()} from {response.url}")
            if response.status != 200:
                logger.error(f"Got unexpected response code {response.status} for {response.url}.")
                return []
            
            r = await response.json()
            if r is None:
                logger.error(f"Got empty response for {response.url}")
                return []
            
            return self.parse_response(r)

    async def _scrape(self, session: aiohttp.ClientSession) -> list:
        data = []
        while True:
            new_data = await self._scrape_page(session)
            if not new_data:
                break

            data += new_data

            if len(new_data) < self.params.limit:
                break

            self.offset += self.params.limit

            await asyncio.sleep(random.random()*self.params.delay*2)
        
        logger.info(f"Processed {len(data)} articles in total.")
        
        return data
    
    async def scrape(self, session: aiohttp.ClientSession) -> pandas.DataFrame:
        date = self.params.from_date
        to_date = self.params.to_date

        data = []
        while date < to_date:
            self.params = self.params._replace(from_date = date, to_date = date+DATE_DELTA)
            data += await self._scrape(session)
            date += DATE_DELTA

        return pandas.DataFrame(data).drop_duplicates("url")
