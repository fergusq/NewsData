from collections import defaultdict, Counter
import json
import os
from typing import Dict, List, NamedTuple
import logging

import pandas as pd

logger = logging.getLogger("tweet_db")

class TweetDatabase(NamedTuple):
    tweets: Dict[str, dict]
    users: Dict[str, dict]
    author_tweets: Dict[str, List[dict]]

    def to_dataframe(self):
        logger.info(f"Preprocessing tweets (phase 3)...")
        df = pd.DataFrame(list(self.tweets.values()))
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True).dt.tz_convert("Europe/Helsinki")
        
        df["retweet_count"] = df["public_metrics"].map(lambda a: a["retweet_count"])
        df["reply_count"] = df["public_metrics"].map(lambda a: a["reply_count"])
        df["like_count"] = df["public_metrics"].map(lambda a: a["like_count"])
        df["quote_count"] = df["public_metrics"].map(lambda a: a["quote_count"])
        del df["retweet_count"]

        df["reply_like_ratio"] = df["reply_count"] / df["like_count"]

        df["replied_to"] = df["referenced_tweets"].map(lambda a: _get_referenced_tweet_id(a, "replied_to"))
        df["retweeted"] = df["referenced_tweets"].map(lambda a: _get_referenced_tweet_id(a, "retweeted"))
        df["quoted"] = df["referenced_tweets"].map(lambda a: _get_referenced_tweet_id(a, "quoted"))
        del df["referenced_tweets"]

        df["repliers"] = df["repliers"].map(dict)
        df["quoters"] = df["quoters"].map(dict)
        df["retweeters"] = df["retweeters"].map(dict)

        df["author_name"] = df["author_id"].map(lambda id: self.users[id]["name"])
        df["author_username"] = df["author_id"].map(lambda id: self.users[id]["username"])

        return df

def _get_referenced_tweet_id(referenced_tweets, type):
    if not referenced_tweets or not isinstance(referenced_tweets, list):
        return None
    
    for rt in referenced_tweets:
        if rt["type"] == type:
            return rt["id"]
    
    return None

def load_tweet_database(*filenames):
    tweets = {}
    users = {}
    for filename in os.listdir("tweets") if not filenames else [fn + ".json" for fn in filenames]:
        logger.info(f"Loading tweet file {filename}...")
        with open(os.path.join("tweets", filename), "r") as f:
            data = json.load(f)

        tweets.update(data["tweets"])
        users.update(data["users"])
    
    logger.info(f"Preprocessing tweets (phase 1)...")
    author_tweets = defaultdict(list)
    for tweet in tweets.values():
        tweet["repliers"] = Counter()
        tweet["quoters"] = Counter()
        tweet["retweeters"] = Counter()
        author_tweets[tweet["author_id"]].append(tweet)
    
    logger.info(f"Preprocessing tweets (phase 2)...")
    for tweet in tweets.values():
        for rt in tweet.get("referenced_tweets", []):
            if rt["id"] in tweets:
                if rt["type"] == "replied_to":
                    tweets[rt["id"]]["repliers"][users[tweet["author_id"]]["username"]] += 1

                elif rt["type"] == "quoted":
                    tweets[rt["id"]]["quoters"][users[tweet["author_id"]]["username"]] += 1

                elif rt["type"] == "retweeted":
                    tweets[rt["id"]]["retweeters"][users[tweet["author_id"]]["username"]] += 1
    
    return TweetDatabase(tweets, users, dict(author_tweets))