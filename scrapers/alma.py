from typing import List
from scrapers.query import PaginatedQuery, QueryResult

class ILQuery(PaginatedQuery):
    MAX_LIMIT = 200
    BASE_URL = "https://iltalehti.fi/"
    API_URL = "https://api.il.fi/v1/articles/search"

    def build_url(self) -> str:
        return self.API_URL
    
    def build_params(self):
        return {
            "date_start": self.params.from_date.strftime("%Y-%m-%d"),
            "date_end": self.params.to_date.strftime("%Y-%m-%d"),
            "q": self.params.query,
            "offset": self.offset,
            "limit": self.params.limit
        }
    
    def parse_response(self, r):
        ans: List[QueryResult] = []

        for a in r["response"]:
            ans.append({
                "url": self.BASE_URL+a["category"]["category_name"]+"/a/"+a["article_id"],
                "title": a["title"],
                "date_modified": a.get("updated_at", None) or a["published_at"],
                "lead": a["lead"],
                "id": a["article_id"],
            })
        
        return ans