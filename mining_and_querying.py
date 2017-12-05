from tweepy import Stream
from tweepy.streaming import StreamListener
import tweepy
from tweepy import OAuthHandler
import json
from pymongo import MongoClient
import pandas as pd


consumer_key = '...'
consumer_secret = '...'
access_token = '...'
access_secret = '...'

auth = OAuthHandler(consumer_key,consumer_secret)
auth.set_access_token(access_token,access_secret)


# listener class 

class listener(StreamListener):

    def on_data(self, data):

        try:
            print('Hi')
            tweet = json.loads(data)

            collection.insert(tweet)

            return True


        except BaseException as e:
            print("Error on_data: %s" % str(e))
            time.sleep(5)
            pass



    def on_error(self, status):

        print (status)

# Setting up database
client = MongoClient()
db = client.barca_tweets
collection = db.twitter_collection

keyword_list = ['#barca','#barcelona','barca']#track list

twitterStream = Stream(auth, listener()) #initialize Stream object with a time out limit
twitterStream.filter(track=keyword_list, languages=['en'])  #call the filter method to run the Stream Listener


# Querying for location from DB

client = MongoClient()
db = client.barca_tweets
collection = db.twitter_collection
loc = []
tweets_iterator = collection.find()
for tweet in tweets_iterator:
    loc.append(tweet['user']['location'])
    
# loc is list of locations of all tweets    

# Creating a dataframe with the list

df = pd.DataFrame(loc,columns = ['location'])
df.to_csv('temporary.csv')# Put this in a separate python file and import 

