import configparser
import datetime
import json
import logging
import sys
from typing import List, Union

import aiohttp
from aiohttp import web

from scrapers import twitter

logger = logging.getLogger("scheduler")

class DailyTwitterScrape:
    def __init__(self, name: str, interval: datetime.timedelta, accounts: List[str] = [], search_words: List[str] = []):
        self.name = name
        self.interval = interval
        self.accounts = accounts
        self.search_words = search_words
    
    async def scrape(self, session: aiohttp.ClientSession, bearer: str):
        today = datetime.date.today()
        date_from = datetime.datetime.combine(today - self.interval, datetime.time(hour=0, minute=0))
        date_to = date_from + datetime.timedelta(days=1)
        tweets = {}
        users = {}
        l = len(self.accounts)
        for i in range(0, l, 10):
            logger.info(f"{i}/{l} Loading tweets from {', '.join(self.accounts[i:i+10])}")
            user_tweets, user_users = await twitter.query_by_username(self.accounts[i:i+10], date_from, date_to, session, bearer)
            tweets.update(user_tweets)
            users.update(user_users)

        for search_word in self.search_words:
            logger.info(f"Loading tweets with {repr(search_word)}")
            search_tweets, search_users = await twitter.query_by_search_word(search_word, date_from, date_to, session, bearer)
            tweets.update(search_tweets)
            users.update(search_users)
        
        filename = f"tweets/{self.name}-{today.isoformat()}.json"
        with open(filename, "w") as f:
            json.dump({"users": users, "tweets": tweets}, f)
        
        logger.info(f"Saved {filename}")
        
        return True

Scraper = Union[DailyTwitterScrape]

def load_list_from_config(section: configparser.SectionProxy, name: str) -> List[str]:
    if section.get(name):
        return list(map(str.strip, section.get("accounts").split(",")))
    
    if section.get(name + "File"):
        filename = section.get(name + "File")
        with open(filename, "r") as f:
            return list(map(str.strip, f.readlines()))
    
    return []

def load_config() -> List[Scraper]:
    parser = configparser.ConfigParser()
    parser.read("scheduler.ini")

    scrapers = []

    for section in parser.sections():
        name = parser[section].get("name", section)
        type = parser[section].get("type", "daily_twitter")
        if type != "daily_twitter":
            logger.error("Unknown scheduled scrape type: " + type)
            continue
        
        accounts = load_list_from_config(parser[section], "accounts")
        search_words = load_list_from_config(parser[section], "searchwords")
        scrapers.append(DailyTwitterScrape(name, datetime.timedelta(days=10), accounts=accounts, search_words=search_words))
    
    return scrapers

async def run_daily_schedule(app: web.Application):
    try:
        scrapers = load_config()
        async with aiohttp.ClientSession(trust_env=True) as session:
            bearer = app["TWITTER_BEARER"]
            for scraper in scrapers:
                logger.info(f"Running scraper {scraper.name}")
                success = await scraper.scrape(session, bearer)
                if not success:
                    logger.info(f"Scraper {scraper.name} was not run")
    
    except:
        logger.error("Failed to run daily schedule", exc_info=sys.exc_info())
