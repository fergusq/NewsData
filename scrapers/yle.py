from datetime import datetime
from typing import List

from scrapers.query import PaginatedQuery, QueryResult, logger as query_logger


class YleQuery(PaginatedQuery):
    MAX_LIMIT = 10000

    def build_url(self):
        return "https://yle-fi-search.api.yle.fi/v1/search"
    
    def build_params(self):
        return {
            "app_id":"hakuylefi_v2_prod",
            "app_key":"4c1422b466ee676e03c4ba9866c0921f",
            "service":"uutiset",
            "language":"fi",
            "uiLanguage":"fi",
            "type":"article",
            "time":"custom",
            "timeFrom": self.params.from_date.strftime("%Y-%m-%d"),
            "timeTo": self.params.to_date.strftime("%Y-%m-%d") if self.params.to_date is not None else datetime.today().strftime("%Y-%m-%d"),
            "query": self.params.query,
            "offset": self.offset,
            "limit": self.params.limit
        }
    
    def parse_response(self, r):
        if r["meta"]["count"] > 10000:
            query_logger.error(f"Query results in {r['meta']['count']} results. The YLE API refuses to return more than 10000 results, so some results are missing. You can work around this limitation by doing multiple queries on smaller timespans.")
        
        ans: List[QueryResult] = []
        for a in r["data"]:
            ans.append({
                "url": a["url"]["full"],
                "title": a["headline"],
                "date_modified": a["datePublished"],
                "id": a["id"],
                "lead": a["lead"]
            })
        
        return ans
