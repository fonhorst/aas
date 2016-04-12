import json
from pymongo import MongoClient
client = MongoClient('192.168.13.133')

isis = client.twitter_datasets.ISIS_TWEET_PARALLEL_FILTERED
original_tweets = isis.find({"retweeted_status": {"$exists": False}}).limit(100)
original_tweets = list(original_tweets)

for tweet in original_tweets:
    tweet['_id'] = str(tweet['_id'])

jtweets = [json.dumps(tweet) + '\n' for tweet in original_tweets]

with open("D:/wspace/data/isis_tiny.data", "w") as f:
    f.writelines(jtweets)