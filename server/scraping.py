import asyncio
import datetime
import logging
import random
import re
from server.tweet_db import load_tweet_database
import sys
from typing import Any, Coroutine, List, NamedTuple, Type

import aiohttp
import databases
import numpy as np
import pandas as pd
import sentiment
from aiohttp import web
import json
from scrapers import fetch, query
from scrapers.alma import ILQuery
from scrapers.sanoma import HSQuery, ISQuery, create_hs_session
from scrapers.twitter import get_tweets_with_url, query_by_username
from scrapers.yle import YleQuery

logger = logging.getLogger("scraping")

finnsentiment_model = sentiment.BinarySentimentAnalyzer("./models/finbert-finnsentiment-v1")

class Sessions(NamedTuple):
    aiohttp_session: aiohttp.ClientSession
    app: web.Application
    db_session: databases.Database

async def get_cached_value(cache_table: str, url: str, db: databases.Database):
    query = f"""
    SELECT content FROM cache WHERE key = :key ORDER BY LENGTH(content) DESC;
    """
    result = await db.fetch_one(query, {"key": cache_table + " " + url})
    return result["content"] if result else None

async def save_cached_value(cache_table: str, url: str, content: str, db: databases.Database):
    await db.execute(f"""
    INSERT INTO cache (key, content) VALUES (:key, :content);
    """, { "key": cache_table + " " + url, "content": content })

def create_scraper(queryClass: Type[query.PaginatedQuery]):
    lock = asyncio.Lock()
    async def scraper(params: query.Params, sessions: Sessions):
        async with lock:
            query = queryClass(params)
            df = await query.scrape(sessions.aiohttp_session)
            if "content" not in params.enabled:
                return df
            
            fetch_results = []
            for url in df["url"]:
                cached = await get_cached_value("scrape_cache", url, sessions.db_session)
                if cached:
                    fetch_results.append(fetch.FetchResult.from_json(cached))
                
                else:
                    fetch_result = await fetch.css_fetch(url, sessions.aiohttp_session)
                    if fetch_result:
                        fetch_results.append(fetch_result)
                        await save_cached_value("scrape_cache", url, fetch_result.to_json(), sessions.db_session)
                    
                    else:
                        fetch_results.append(fetch.FetchResult(content="", persons=[]))
                    
                    await asyncio.sleep(random.random()*2)
            
            df["content"] = [r.content for r in fetch_results]
            df["persons"] = [r.persons for r in fetch_results]

        coroutines = []

        if "parser" in params.enabled and sessions.app["PARSER_ENABLED"]:
            coroutines.append(parse_to_conllu(df, sessions))
        
        if "ner" in params.enabled and sessions.app["NER_ENABLED"]:
            coroutines.append(get_named_entities(df, sessions))
        
        if "twitter" in params.enabled and sessions.app["TWITTER_ENABLED"]:
            coroutines.append(get_tweets(df, sessions))

        if "sentiment" in params.enabled:
            coroutines.append(predict_sentiment(df))
    
        if "annif" in params.enabled and sessions.app["ANNIF_ENABLED"]:
            coroutines.append(predict_subjects(df, sessions))
        
        await asyncio.gather(*coroutines)

        return df
    
    return scraper

hs_lock = asyncio.Lock()
async def hs_scraper(params: query.Params, sessions: Sessions):
    async with hs_lock:
        query = HSQuery(params)
        df = await query.scrape(sessions.aiohttp_session)
        if "content" not in params.enabled:
            return df
        
        fetch_results = []
        async with create_hs_session(sessions.app["HS_USERNAME"], sessions.app["HS_PASSWORD"]) as hs_fetch:
            for url in df["url"]:
                cached = await get_cached_value("scrape_cache", url, sessions.db_session)
                if cached:
                    fetch_results.append(fetch.FetchResult.from_json(cached))
                
                else:
                    fetch_result = await hs_fetch.fetch_hs(url)
                    if fetch_result:
                        fetch_results.append(fetch_result)
                        await save_cached_value("scrape_cache", url, fetch_result.to_json(), sessions.db_session)
                    
                    else:
                        fetch_results.append(fetch.FetchResult(content="", persons=[]))
                    
                    await asyncio.sleep(1+random.random()*2)
        
        df["content"] = [r.content for r in fetch_results]
        df["persons"] = [r.persons for r in fetch_results]

    coroutines = []

    if "parser" in params.enabled and sessions.app["PARSER_ENABLED"]:
        coroutines.append(parse_to_conllu(df, sessions))
    
    if "ner" in params.enabled and sessions.app["NER_ENABLED"]:
        coroutines.append(get_named_entities(df, sessions))
    
    if "twitter" in params.enabled and sessions.app["TWITTER_ENABLED"]:
        coroutines.append(get_tweets(df, sessions))

    if "sentiment" in params.enabled:
        coroutines.append(predict_sentiment(df))
    
    if "annif" in params.enabled and sessions.app["ANNIF_ENABLED"]:
        coroutines.append(predict_subjects(df, sessions))
    
    await asyncio.gather(*coroutines)

    return df

tweet_lock = asyncio.Lock()
async def get_tweets(df: pd.DataFrame, sessions: Sessions):
    async with tweet_lock:
        tweets_column = []
        sentiment_column = []
        for i, (url, date_modified) in enumerate(zip(df["url"], df["date_modified"])):
            tweets = []
            sentiments = []
            logger.info(f"({i+1}/{len(df)}) Getting tweets with {url} ({date_modified})")
            try:
                date_modified = pd.to_datetime(date_modified, utc=True)
                date_modified = datetime.datetime.combine(date_modified.date(), date_modified.time())
                cache_key = f"get_tweets_with_url({url}, {date_modified} +- 1 week)"
                cached = await get_cached_value("tweet_cache", cache_key, sessions.db_session)
                if cached:
                    tweets = json.loads(cached)
                
                else:
                    tweets = await get_tweets_with_url(
                        url,
                        start_date=date_modified-datetime.timedelta(weeks=1),
                        end_date=date_modified+datetime.timedelta(weeks=1),
                        session=sessions.aiohttp_session,
                        bearer=sessions.app["TWITTER_BEARER"]
                    )
                    if tweets:
                        await save_cached_value("tweet_cache", cache_key, json.dumps(tweets), sessions.db_session)

                logger.info(f"({i+1}/{len(df)}) Calculating sentiments for tweets with {url} ({date_modified})")

                for j, tweet in enumerate(tweets):
                    sentences = re.split("[.!?] ", tweet["text"])
                    sentiments.append(np.mean((finnsentiment_model.predict(sentences)*[-1, 0, 1]).sum(-1)))
                    await asyncio.sleep(0)
            
            except:
                logger.error("Error during fetching tweets", exc_info=sys.exc_info())
            
            tweets_column.append(tweets)
            sentiment_column.append(sentiments)
            await asyncio.sleep(3.1)
    
        df["tweets"] = tweets_column
        df["tweet_sentiments"] = sentiment_column

async def parse_to_conllu(df: pd.DataFrame, sessions: Sessions):
    conllus = []
    for i, (url, content) in enumerate(zip(df["url"], df["content"])):
        logger.info(f"({i+1}/{len(df)}) Parsing text from {url}")
        if not isinstance(content, str):
            logger.warning(f"The content of {url} is not str, it is {content}")
        
        conllu = ""
        try:
            cached = await get_cached_value("parser_cache", url, sessions.db_session)
            if cached:
                conllu = cached
            
            else:
                async with sessions.aiohttp_session.post(sessions.app["PARSER_URL"], data=content.encode("utf-8"), headers={"Content-Type": "text/plain; charset=utf-8"}) as response:
                    conllu = await response.text()
                
                await save_cached_value("parser_cache", url, conllu, sessions.db_session)
        
        except:
            logger.error("Error during parsing", exc_info=sys.exc_info())
        
        conllus.append(conllu)
    
    df["conllu"] = conllus

async def get_named_entities(df: pd.DataFrame, sessions: Sessions):
    entities_column = []
    for i, (url, content) in enumerate(zip(df["url"], df["content"])):
        if not isinstance(content, str):
            logger.warning(f"The content of {url} is not str, it is {content}")
        
        cached = await get_cached_value("ner_cache", url, sessions.db_session)
        if cached:
            entities = json.loads(cached)
            if entities:
                logger.info(f"({i+1}/{len(df)}) Using cached NER entities for {url}")
                entities_column.append(entities)
                continue
        
        entities = []
        try:
            logger.info(f"({i+1}/{len(df)}) NER tagging text from {url}")
            for line in content.split("\n"):
                line = line.strip()
                if not line:
                    continue
                
                if len(line) > 4090:
                    logger.warning(f"Too long line for NER tagging: {len(line)}")
                    line = line[:4090] # liian pitkä rivi käsiteltäväksi, pitää leikata :(
                
                async with sessions.aiohttp_session.post(sessions.app["NER_URL"], params={"text": line}, headers={"Content-Type": "text/plain; charset=utf-8"}) as resp:
                    data = await resp.json()
                
                entity = None
                for sentence in data:
                    for [form, lemma, analysis, ner_analysis, ner_tag, _, _, _] in sentence:
                        if entity:
                            entity += " " + lemma
                        
                        if re.fullmatch(r"<\w+/>", ner_tag):
                            entities.append((ner_tag[1:-2], lemma))
                        
                        elif re.fullmatch(r"<(\w+)>", ner_tag):
                            entity = lemma
                        
                        elif re.fullmatch(r"</(\w+)>", ner_tag):
                            entities.append((ner_tag[2:-1], entity))
                            entity = None
                
            await save_cached_value("ner_cache", url, json.dumps(entities), sessions.db_session)
        
        except:
            logger.error("Error during NER tagging", exc_info=sys.exc_info())
        
        entities_column.append(entities)
    
    df["entities"] = entities_column

async def predict_subjects(df: pd.DataFrame, sessions: Sessions):
    subject_column = []
    for i, (url, content) in enumerate(zip(df["url"], df["content"])):
        logger.info(f"({i+1}/{len(df)}) Predicting subjects for {url}")
        if not isinstance(content, str):
            logger.warning(f"The content of {url} is not str, it is {content}")
        
        uris = []
        subjects = ""
        try:
            cached = await get_cached_value("subject_cache", url, sessions.db_session)
            if cached:
                subjects = cached
            
            else:
                params = {
                    "text": content,
                    "limit": 15,
                    "threshold": 0.2,
                }
                annif_url = f"{sessions.app['ANNIF_URL']}"
                async with sessions.aiohttp_session.post(annif_url, data=params) as response:
                    subjects = await response.text()
                
                await save_cached_value("subject_cache", url, subjects, sessions.db_session)
                
            for result in json.loads(subjects)["results"]:
                uri = f"<{result['uri']}>"
                uris.append(uri)
        
        except:
            logger.error("Error during subject prediction", exc_info=sys.exc_info())
        
        subject_column.append(uris)
    
    df["subjects"] = subject_column

async def predict_sentiment(df: pd.DataFrame):
    sentiment_column = []
    for i, (url, content) in enumerate(zip(df["url"], df["content"])):
        try:
            logger.info(f"({i+1}/{len(df)}) Calculating sentiments for {url}")
            sentences = re.split("[.!?] ", content)
            sentiment_column.append(np.mean((finnsentiment_model.predict(sentences)*[-1, 0, 1]).sum(-1)))
            await asyncio.sleep(0)
    
        except:
            logger.info(f"Skipping sentiment for {url}")
            sentiment_column.append(np.nan)
    df["sentiment"] = sentiment_column

async def twitter_scraper(params: query.Params, sessions: Sessions):
    logger.info("Loading tweet database...")
    tweets: Any = load_tweet_database(*params.extra.get("scrape_ids", [])).to_dataframe()
    await asyncio.sleep(0)

    logger.info("Filtering tweets...")
    from_datetime = datetime.datetime.combine(params.from_date, datetime.time(0, 0, 0)).isoformat()
    to_datetime = datetime.datetime.combine(params.to_date, datetime.time(23, 59, 59)).isoformat()
    tweets = tweets[(from_datetime <= tweets.created_at) & (tweets.created_at <= to_datetime)]
    await asyncio.sleep(0)

    if params.extra.get("drop_retweets", False):
        tweets.drop(tweets[~tweets["retweeted"].isna()].index, inplace=True)

    if "accounts" in params.extra:
        author = params.extra["accounts"]
        if not isinstance(author, list):
            author = [author]
        
        tweets = tweets[tweets.author_username.map(lambda u: u in author)]
        await asyncio.sleep(0)

    if params.query:
        tweets = tweets[tweets.content.str.contains(params.query)]
        await asyncio.sleep(0)
    
    logger.info("Sampling tweets...")
    if params.extra.get("sample", 0) > 0:
        tweets = tweets.sample(params.extra["sample"])
    
    logger.info("Preprocessing tweets...")
    tweets["url"] = tweets.id.map(lambda i: f"twitter:{i}")
    tweets["content"] = tweets.text
    del tweets["text"]
    tweets["persons"] = tweets.content.map(lambda text: re.findall(r"@\w+", text))
    tweets["hashtags"] = tweets.content.map(lambda text: re.findall(r"#\w+", text))
    await asyncio.sleep(0)

    logger.info("Preprocessing tweets...")
    if sessions.app["TWITTER_METADATA"]:
        metadata = pd.read_csv(str(sessions.app["TWITTER_METADATA"]))
        tweets = pd.merge(tweets, metadata, left_on="author_username", right_on="twitter", how="left")

    coroutines = []

    if "parser" in params.enabled and sessions.app["PARSER_ENABLED"]:
        coroutines.append(parse_to_conllu(tweets, sessions))
    
    if "ner" in params.enabled and sessions.app["NER_ENABLED"]:
        coroutines.append(get_named_entities(tweets, sessions))
    
    #if "twitter" in params.enabled and sessions.app["TWITTER_ENABLED"]:
    #    coroutines.append(get_tweets(tweets, sessions))

    if "sentiment" in params.enabled:
        coroutines.append(predict_sentiment(tweets))
    
    if "annif" in params.enabled and sessions.app["ANNIF_ENABLED"]:
        coroutines.append(predict_subjects(tweets, sessions))
    
    await asyncio.gather(*coroutines)

    return tweets

SCRAPERS = {
    "il": create_scraper(ILQuery),
    "is": create_scraper(ISQuery),
    "yle": create_scraper(YleQuery),
    "hs": hs_scraper,
    "twitter": twitter_scraper,
}

async def start_scraping(params: query.Params, media: List[str], ticket_id: str, app: web.Application):
    db: databases.Database = app["db"]
    try:
        logger.info(f"Scrape {ticket_id} started")
        async with aiohttp.ClientSession(trust_env=True) as session:
            sessions = Sessions(session, app, db)
            dataframeFutures: List[Coroutine[Any, Any, pd.DataFrame]] = []
            for media_name in media:
                if media_name in SCRAPERS:
                    dataframeFutures.append(SCRAPERS[media_name](params, sessions))

                else:
                    logger.warning(f"Unknown media {media_name}")
            
            df = pd.concat(await asyncio.gather(*dataframeFutures))
        
        logger.info(f"Scrape {ticket_id} finished")

        resource_id = ticket_id
        await db.execute("""INSERT INTO resources(uuid, resource, date) VALUES (:id, :content, datetime('now'));""", {"id": resource_id, "content": df.to_csv()})
        await db.execute("""UPDATE tickets SET resource_id = :resource_id WHERE uuid = :ticket_id;""", {"resource_id": resource_id, "ticket_id": ticket_id})
        await db.execute("""UPDATE tickets SET status = 'finished' WHERE uuid = :id;""", {"id": ticket_id})
    except:
        logger.error("Error during scraping", exc_info=sys.exc_info())
        await db.execute("""UPDATE tickets SET status = 'error' WHERE uuid = :id;""", {"id": ticket_id})

async def start_scraping_twitter(accounts: List[str], date_from: datetime.datetime, date_to: datetime.datetime, ticket_id: str, app: web.Application):
    db: databases.Database = app["db"]
    try:
        logger.info(f"Scrape {ticket_id} started")
        tweets = {}
        users = {}
        l = len(accounts)
        async with aiohttp.ClientSession() as session:
            for i in range(0, l, 10):
                logger.info(f"{i}/{l} Loading tweets from {', '.join(accounts[i:i+10])}")
                for _ in range(3):
                    try:
                        user_tweets, user_users = await query_by_username(accounts[i:i+10], date_from, date_to, session, app["TWITTER_BEARER"])
                        tweets.update(user_tweets)
                        users.update(user_users)
                        break
                    except Exception as ex:
                        sys.stderr.write(str(ex) + "\n")
                        await asyncio.sleep(3)
        
        with open(f"tweets/{ticket_id}.json", "w") as f:
            json.dump({"tweets": tweets, "users": users}, f)
        
        logger.info(f"Scrape {ticket_id} finished")
        await db.execute("""UPDATE tickets SET status = 'finished' WHERE uuid = :id;""", {"id": ticket_id})
    except:
        logger.error("Error during scraping", exc_info=sys.exc_info())
        await db.execute("""UPDATE tickets SET status = 'error' WHERE uuid = :id;""", {"id": ticket_id})
