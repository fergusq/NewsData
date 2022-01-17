import ast
from collections import defaultdict
import json
import re
import os
from server.tweet_db import TweetDatabase, load_tweet_database
from typing import Dict, List, NamedTuple

import numpy as np
import pandas as pd
from aiohttp import web
from multidict import MultiDictProxy

OUTPUT_COLUMNS = ["date_modified", "url", "title", "media", "n_tweets", "n_words", "n_persons", "tweet_sentiment_avg", "tweet_sentiment_sum", "tweet_sentiment_abs_sum", "tweet_controversiality"]

MEDIAS = [
    (r"hs\.fi", "HS"),
    (r"is\.fi", "IS"),
    (r"iltalehti\.fi", "IL"),
    (r"yle\.fi", "Yle"),
]

def _identify_media(url):
    for pattern, name in MEDIAS:
        if re.search(pattern, url):
            return name
    
    return "tuntematon"

def _preprocess_data(data: pd.DataFrame) -> pd.DataFrame:
    data["date_modified"] = pd.to_datetime(data["date_modified"], utc=True).dt.tz_convert("Europe/Helsinki")
    if "content" in data:
        data["content"] = data["content"].map(str)
        data["n_words"] = data["content"].str.split(r"\s+").map(len)
        data["n_persons"] = data["persons"].map(len)
    
    if "entities" in data:
        data["entities"] = data["entities"].map(ast.literal_eval)
    
    if "tweets" in data:
        data["tweets"] = data["tweets"].map(ast.literal_eval)
        data["tweet_sentiments"] = data["tweet_sentiments"].map(ast.literal_eval)
        data["tweet_sentiment_avg"] = data["tweet_sentiments"].map(np.mean)
        data["tweet_sentiment_sum"] = data["tweet_sentiments"].map(np.sum)
        data["tweet_sentiment_abs_sum"] = data["tweet_sentiments"].map(lambda s: np.sum(np.abs(s)))
        data["tweet_controversiality"] = data.tweet_sentiment_abs_sum - np.abs(data.tweet_sentiment_sum)
        data["n_tweets"] = data["tweets"].map(len)
    
    data["media"] = data["url"].map(_identify_media)
    return data

def count_matches(data: pd.DataFrame, params: MultiDictProxy[str]) -> pd.DataFrame:
    data = data.fillna("")
    keys = []

    patterns = params.getall("regex", [])
    for pattern in patterns:
        keys.append("regex_"+pattern)
        data["regex_"+pattern] = data["content"].str.contains(pattern).map(int)

    patterns = params.getall("iregex", [])
    for pattern in patterns:
        keys.append("iregex_"+pattern)
        data["iregex_"+pattern] = data["content"].str.lower().str.contains(pattern).map(int)

    patterns = params.getall("ner", [])
    for pattern in patterns:
        keys.append("ner_"+pattern)
        if "::" in pattern:
            p = tuple(pattern.lower().split("::"))
            matcher = lambda l: any(tuple(i) == p for i in l)
        
        else:
            matcher = lambda l: any(i[1] == pattern.lower() for i in l)
        
        data["ner_"+pattern] = data["entities"].map(matcher).map(int)
    
    return data[OUTPUT_COLUMNS+keys]

def article_list(data: pd.DataFrame, params: MultiDictProxy[str]) -> pd.DataFrame:
    return data[OUTPUT_COLUMNS]

def named_entities(data: pd.DataFrame, params: MultiDictProxy[str]) -> pd.DataFrame:
    ans = []
    for date_modified, media, entities in zip(data.date_modified, data.media, data.entities):
        for etype, ename in entities:
            ans.append({
                "date": date_modified,
                "media": media,
                "type": etype,
                "entity": ename,
            })
    
    return pd.DataFrame(ans)

METHODS = {
    "count_matches": count_matches,
    "article_list": article_list,
    "named_entities": named_entities,
}

def analyze(data: pd.DataFrame, method: str, params: MultiDictProxy[str]) -> pd.DataFrame:
    if method not in METHODS:
        raise web.HTTPNotFound()
    
    return METHODS[method](_preprocess_data(data), params)

def twitter_count_matches(data: TweetDatabase, params: MultiDictProxy[str]) -> pd.DataFrame:
    df = data.to_dataframe()
    keys = []

    patterns = params.getall("regex", [])
    for pattern in patterns:
        keys.append("regex_"+pattern)
        df["regex_"+pattern] = df["text"].str.contains(pattern).map(int)

    patterns = params.getall("iregex", [])
    for pattern in patterns:
        keys.append("iregex_"+pattern)
        df["iregex_"+pattern] = df["text"].str.lower().str.contains(pattern).map(int)
    
    return df[["created_at"]+keys]

TWITTER_METHODS = {
    "count_matches": twitter_count_matches,
}

def analyze_tweets(method: str, params: MultiDictProxy[str]) -> pd.DataFrame:
    if method not in TWITTER_METHODS:
        raise web.HTTPNotFound()
    
    return TWITTER_METHODS[method](load_tweet_database(), params)