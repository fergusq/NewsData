import asyncio
import datetime
import re
import sys
from typing import List, Tuple
import aiohttp
import logging

logger = logging.getLogger("twitter_scraper")

twitter_lock = asyncio.Lock()

async def get_tweets_with_url(url: str, start_date: datetime.datetime, end_date: datetime.datetime, session: aiohttp.ClientSession, bearer: str) -> List[dict]:
    if '"' in url:
        logger.error(f"Illegal characters in url: {url}")
        return []
    
    tweets, _users = await query_tweets(f"url:\"{url}\" lang:fi", start_date, end_date, session, bearer, included_tweets=False)
    return list(tweets.values())

async def query_by_username(usernames: List[str], start_date: datetime.datetime, end_date: datetime.datetime, session: aiohttp.ClientSession, bearer: str) -> Tuple[dict, dict]:
    for username in usernames:
        if not re.fullmatch(r"[a-zA-Z0-9_]{1,15}", username):
            logger.error(f"Illegal characters in Twitter username: {username}")
            return ({}, {})
    
    query = " OR ".join(f"from:\"{username}\"" for username in usernames)
    return await query_tweets(f"({query}) lang:fi", start_date, end_date, session, bearer, included_tweets=False)

async def query_by_search_word(search_word: str, start_date: datetime.datetime, end_date: datetime.datetime, session: aiohttp.ClientSession, bearer: str) -> Tuple[dict, dict]:
    if '"' in search_word:
        logger.error(f"Illegal characters in search word: {search_word}")
        return ({}, {})
    
    return await query_tweets(f"\"{search_word}\" lang:fi", start_date, end_date, session, bearer, included_tweets=False)

async def query_tweets(
    query: str,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    session: aiohttp.ClientSession,
    bearer: str,
    included_tweets=True,
) -> Tuple[dict, dict]:
    tweets = {}
    users = {}

    for data in await _query_tweets(query, start_time, end_time, session, bearer):
        for tweet in data.get("data", []):
            tweets[tweet["id"]] = tweet

        if included_tweets:
            for tweet in data.get("includes", {}).get("tweets", []):
                tweets[tweet["id"]] = tweet

        for user in data.get("includes", {}).get("users", []):
            users[user["id"]] = user
    
    return tweets, users

async def _query_tweets(
    query: str,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    session: aiohttp.ClientSession,
    bearer: str,
    next_token=None, print=print, count=0
) -> List[dict]:
    params = {
        "query": query,
        "tweet.fields": "created_at,public_metrics,author_id,in_reply_to_user_id,referenced_tweets",
        "expansions": "referenced_tweets.id.author_id,author_id",
        "max_results": 500,
        "start_time": start_time.astimezone().isoformat(),
        "end_time": end_time.astimezone().isoformat(),
    }
    if next_token:
        params["next_token"] = next_token
    
    async with twitter_lock:
    
        res = await session.get("https://api.twitter.com/2/tweets/search/all", params=params, headers={"Authorization": f"Bearer {bearer}"})
        await asyncio.sleep(3.1)
        if res:
            while res.status == 503:
                logger.info(f"Twitter error 503 - Retrying...")
                res = await session.get("https://api.twitter.com/2/tweets/search/all", params=params, headers={"Authorization": f"Bearer {bearer}"})
                await asyncio.sleep(3.1)
            
            if res.status != 200:
                logger.error(f"Twitter error {res.status}")
                logger.info(res.content)
                return []
        
        else:
            logger.warning("Returning [] from twitter query due to errors...")
            return []
        
    results = await res.json()
    if "meta" not in results:
        logger.error(f"Illegal Twitter response: {res.content}")
        return []
    
    if "next_token" in results["meta"]:
        logger.info(f"Pagination required... {count+results['meta'].get('result_count', 0)}")
        return [results] + await _query_tweets(query, start_time, end_time, session, bearer, next_token=results["meta"]["next_token"], count=count+results["meta"].get("result_count", 0))
    
    return [results]

async def retry_get(session: aiohttp.ClientSession, retry_times: int, *args, **kwargs):
    try:
        return await session.get(*args, **kwargs)
    
    except:
        logger.warning("Error during querying twitter", exc_info=sys.exc_info())
        if retry_times > 0:
            logger.info("Retrying...")
            return retry_get(session, retry_times - 1, *args, **kwargs)
        
        else:
            return None