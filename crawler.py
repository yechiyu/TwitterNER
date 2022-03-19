import tweepy
import json
from pymongo import MongoClient
# from datetime import datetime
import time
# import sys
import emoji
import re
# import requests
from bson import json_util
#  please put your credentials below - very important

consumer_key = "iZC6veei76gDMGAJS46hQUHPg"
consumer_secret = "kcm4oIsquusXPs30SpQr8IzvB2EANKyrU62FqnFguHqeu5avHy"
access_token = "1202150312712253441-iXclTlTKojpWWCSVmLyXSH0fmk1i6i"
access_token_secret = "lJuANij9ov8HkIB2rpF9isAAelg0tqZSjNyPNqMeLPhME"
'''
consumer_key = 'DiLHq2Pni5O3F37BouYTg0Xu6'
consumer_secret = 'vb2I3rn7XIUkpN239g9nEo7anoWCRhU8tgU5cKqZTerlkrEafY'
access_token = '1170725265883959299-ZrqCn7VRy7nM19sn8FwL2oovc1cvxS'
access_token_secret = 'zyZ6ZnFuLndgMGH9gild9FqI1UUrzZ2NnEZo2qv29jwgf'

consumer_key = '4CjjMhjNlFAeApgzjUicwADqv'
consumer_secret = 'pTt0mP6z2TxJdCy9oMnltf5Akd3a7NhLijGXK6WajJJsFzhiht'
access_token = '1193078882066296832-VDR3A5aIbuPpIyQtxxe4aUZSQhSfYp'
access_token_secret = 'UjAKqltTo7QTdYxcv44Pe09tlYUaF5fx9BiyEBxhFuyhd'

consumer_key = 'RaB095J4xQTED7xBCWYQ2nufy'
consumer_secret = 'zPFHxgZLyZEq1af0anOWze2a6LB1nHZgzKFS130ZFGme0BcZUb'
access_token = '1374723615191621632-9h5teePVDj5I2YgvfuCa4FfjYMGp9t'
access_token_secret = '7tiW0v2g66CSarDv0oxiEjNOeqhPM3dnrCnhE7DNWUO9S'

consumer_key = "iZC6veei76gDMGAJS46hQUHPg"
consumer_secret = "kcm4oIsquusXPs30SpQr8IzvB2EANKyrU62FqnFguHqeu5avHy"
access_token = "1202150312712253441-iXclTlTKojpWWCSVmLyXSH0fmk1i6i"
access_token_secret = "lJuANij9ov8HkIB2rpF9isAAelg0tqZSjNyPNqMeLPhME"

'''

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)
if not api:
    print('Can\'t authenticate')
    print('failed consumer id ----------: ', consumer_key)
# set DB DETAILS


# this is to setup local Mongodb
client = MongoClient('127.0.0.1', 27017)  # is assigned local port
dbName = "TwitterDump"  # set-up a MongoDatabase
db = client[dbName]
collName1 = 'colTest1'  # here we create a collection
collName2 = 'colTest2'
collName4 = 'colTest4'
collName5 = 'colTest5'
collName6 = 'colTest6'
collection = db[collName1]  # This is for the Collection  put in the DB
collection1 = db[collName2]
collection3 = db[collName4]
collection4 = db[collName5]
collection5 = db[collName6]


def strip_emoji(text):
    new_text = re.sub(emoji.get_emoji_regexp(), r"", text)
    return new_text


def cleanList(text):
    # remove emoji it works
    text = strip_emoji(text)
    text.encode("ascii", errors="ignore").decode()

    return text


def processTweets(tweet):
    #  this module is for cleaning text and also extracting relevant twitter feilds
    # initialise placeholders
    place_countrycode = None
    place_name = None
    place_country = None
    place_coordinates = None
    source = None
    exactcoord = None
    place = None
    videolinks = None

    # Pull important data from the tweet to store in the database.
    try:
        created = tweet['created_at']
        tweet_id = tweet['id']  # The Tweet ID from Twitter in string format
        username = tweet['user']['screen_name']  # The username of the Tweet author
        quote = tweet['is_quote_status']
        reply = tweet['in_reply_to_status_id']
        # followers = t['user']['followers_count']  # The number of followers the Tweet author has
        text = tweet['text']  # The entire body of the Tweet
        verified = tweet['user']['verified']
        use_followers_count = tweet['user']["followers_count"]
        use_retweet_count = tweet['retweet_count']
        # media = tweet['includes']['media']['type']
        user_time = tweet['user']['created_at']
        user_profile = tweet['user']['default_profile_image']
        # media = tweet['includes']['media']['type']
        user_description = tweet['user']['description']

        retweet = False
    except Exception as e:
        # if this happens, there is something wrong with JSON, so ignore this tweet
        print(e)
        return None

    try:
        videoUrl = tweet['extended_entities']["media"][0]["video_info"]
        if videoUrl:
            videolinks = videoUrl["variants"][0]["url"]
    except Exception as e:
        print("no video in this tweet\n")

    try:
        # // deal with truncated
        if tweet['truncated']:
            text = tweet['extended_tweet']['full_text']
        elif text.startswith('RT'):
            # print(' tweet starts with RT **********')
            retweet = True
            try:
                if tweet['retweeted_status']['truncated']:
                    # print("in .... tweet.retweeted_status.truncated == True ")
                    text = tweet['retweeted_status']['extended_tweet']['full_text']
                    # print(text)
                else:
                    text = tweet['retweeted_status']['full_text']
            except Exception as e:
                pass
    except Exception as e:
        print(e)
    # print(text)
    text = cleanList(text)
    # print(text)

    # print(entities)

    entities = tweet['entities']
    mediaList = []
    mediaUrlList = []
    try:
        if entities['media']:
            medias = entities['media']
            for x in medias:
                mediaList.append(x['type'])
                mediaUrlList.append(x['media_url'])
    except Exception as e:
        print("no photo in this tweet")

    mentions = entities['user_mentions']
    mList = []

    for x in mentions:
        # print(x['screen_name'])
        mList.append(x['screen_name'])
    hashtags = entities['hashtags']  # Any hashtags used in the Tweet
    hList = []
    for x in hashtags:
        # print(x['screen_name'])
        hList.append(x['text'])
    # if hashtags == []:
    #     hashtags =''
    # else:
    #     hashtags = str(hashtags).strip('[]')
    source = tweet['source']
    exactcoord = tweet['coordinates']
    coordinates = None
    if exactcoord:
        # print(exactcoord)
        coordinates = exactcoord['coordinates']
        # print(coordinates)
    geoenabled = tweet['user']['geo_enabled']
    location = tweet['user']['location']

    if geoenabled and (text.startswith('RT') == False):
        # print(tweet)
        # sys.exit() # (tweet['geo']):
        try:
            if tweet['place']:
                # print(tweet['place'])
                place_name = tweet['place']['full_name']
                place_country = tweet['place']['country']
                place_countrycode = tweet['place']['country_code']
                place_coordinates = tweet['place']['bounding_box']['coordinates']
        except Exception as e:
            print(e)
            print('error from place details - maybe AttributeError: ... NoneType ... object has no attribute '
                  '..full_name ...')
    tweet1 = {'uid': tweet_id, 'date': created, 'username': username, 'text': text, 'geoenabled': geoenabled,
              'coordinates': coordinates, 'location': location, 'place_name': place_name,
              'place_country': place_country, 'country_code': place_countrycode,
              'place_coordinates': place_coordinates, 'followers_count': use_followers_count,
              'retweet_count': use_retweet_count,
              'hashtags': hList, 'mentions': mList, 'source': source, 'quote': quote, 'reply': reply,
              'retweet': retweet, 'verified': verified, 'media': mediaList, 'mediaUrl': mediaUrlList,
              'videoLinks': videolinks,'user_time': user_time, 'user_profile': user_profile
              , 'description': user_description}
    return tweet1


# if tweet['id'] not in tweets:
#     tweets.append(tweet['id'])

def processTweets1(tweet):
    #  this module is for cleaning text and also extracting relevant twitter feilds
    # initialise placeholders
    place_countrycode = None
    place_name = None
    place_country = None
    place_coordinates = None
    # source = None
    # exactcoord = None
    # place = None
    # videolinks = None

    # print(t)

    # Pull important data from the tweet to store in the database.

    try:
        created = tweet['created_at']
        tweet_id = tweet['id']  # The Tweet ID from Twitter in string format
        username = tweet['user']['screen_name']  # The username of the Tweet author
        quote = tweet['is_quote_status']
        reply = tweet['in_reply_to_status_id']
        # followers = t['user']['followers_count']  # The number of followers the Tweet author has
        text = tweet['full_text']  # The entire body of the Tweet
        verified = tweet['user']['verified']
        # media = tweet['includes']['media']['type']
        use_followers_count = tweet['user']["followers_count"]
        use_retweet_count = tweet['retweet_count']
        retweet = False
    except Exception as e:
        # if this happens, there is something wrong with JSON, so ignore this tweet
        print(e)
        return None

        # try:
        #     videoUrl = tweet['extended_entities']["media"][0]["video_info"]
        #     if videoUrl:
        #         videolinks = videoUrl["variants"][0]["url"]
        # except Exception as e:
        #     print(e)

    try:
        # // deal with truncated
        if tweet['truncated']:
            text = tweet['extended_tweet']['full_text']
        elif text.startswith('RT'):
            # print(' tweet starts with RT **********')
            retweet = True
            try:
                if tweet['retweeted_status']['truncated']:
                    # print("in .... tweet.retweeted_status.truncated == True ")
                    text = tweet['retweeted_status']['extended_tweet']['full_text']
                # print(text)
                else:
                    text = tweet['retweeted_status']['full_text']
            except Exception as e:
                pass
    except Exception as e:
        print(e)
        # print(text)
    text = cleanList(text)
    # print(text)

    # print(entities)

    entities = tweet['entities']
    mentions = entities['user_mentions']
    mList = []

    for x in mentions:
        # print(x['screen_name'])
        mList.append(x['screen_name'])
    hashtags = entities['hashtags']  # Any hashtags used in the Tweet
    hList = []
    for x in hashtags:
        # print(x['screen_name'])
        hList.append(x['text'])
        # if hashtags == []:
        #     hashtags =''
        # else:
        #     hashtags = str(hashtags).strip('[]')
    source = tweet['source']
    exactcoord = tweet['coordinates']
    coordinates = None
    if exactcoord:
        # print(exactcoord)
        coordinates = exactcoord['coordinates']
        # print(coordinates)
    geoenabled = tweet['user']['geo_enabled']
    location = tweet['user']['location']

    if geoenabled and (text.startswith('RT') == False):
        # print(tweet)
        # sys.exit() # (tweet['geo']):
        try:
            if tweet['place']:
                # print(tweet['place'])
                place_name = tweet['place']['full_name']
                place_country = tweet['place']['country']
                place_countrycode = tweet['place']['country_code']
                place_coordinates = tweet['place']['bounding_box']['coordinates']
        except Exception as e:
            print(e)
            print('error from place details - maybe AttributeError: ... NoneType ... object has no attribute '
                  '..full_name ...')

    tweet1 = {'uid': tweet_id, 'date': created, 'username': username, 'text': text, 'geoenabled': geoenabled,
              'coordinates': coordinates, 'location': location, 'place_name': place_name,
              'place_country': place_country, 'country_code': place_countrycode, 'followers_count': use_followers_count,
              'retweet_count': use_retweet_count,
              'place_coordinates': place_coordinates, 'hashtags': hList, 'mentions': mList, 'source': source,
              'quote': quote, 'reply': reply, 'retweet': retweet, 'verified': verified}
    # 'media': mediaList, 'mediaUrl': mediaUrlList,"videoLinks": videolinks
    return tweet1


class StreamListener(tweepy.StreamListener):
    # This is a class provided by tweepy to access the Twitter Streaming API.
    global geoEnabled
    global geoDisabled

    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")

    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print('An Error has occurred: ' + repr(status_code))
        return False

    def on_data(self, data):
        # This is where each tweet is collected
        # let us load the  json data
        t = json.loads(data)
        #  now let us process the tweet so that we will deal with cleaned and extracted JSON
        tweet = processTweets(t)
        print("streaming api:", tweet)
        # now insert it
        #  for this to work you need to start a local mongodb server
        try:
            collection5.insert_one(tweet)
        except Exception as e:
            print(e)
        try:
            with open("./original_data/stream.json", "a") as f:
                f.writelines(json_util.dumps(tweet))
                f.write('\n')
                print("Loading ini the file ...")
        except Exception as e:
            print(e)
            # this means some Mongo db insertion error


# Set up the listener. The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.

# WORDS = ['manhattan' , 'new york city', 'statue of liberty']
# LOCATIONS = [ -75,40,-72,42] # new york city
Loc_UK = [-10.392627, 49.681847, 1.055039, 61.122019]  # UK and Ireland
'''
Words_UK = ["Boris", "Prime Minister", "Tories", "UK", "London", "England", "Manchester", "Sheffield", "York",
            "Southampton","Wales", "Cardiff", "Swansea", "Banff", "Bristol", "Oxford", "Birmingham", "Scotland",
            "Glasgow","Edinburgh", "Dundee", "Aberdeen", "Highlands", "Inverness", "Perth", "St Andrews", "Dumfries",
            "Ayr", "Ireland", "Dublin","Cork", "Limerick", "Galway", "Belfast", " Derry", "Armagh","BoJo", "Labour",
            "Liberal Democrats", "SNP","Conservatives", "First Minister", "Surgeon", "Chancelor","Boris Johnson",
            "BoJo", "Keith Stramer"]
'''
Words_UK = ["Boris", "Prime Minister", "coronavirus", "2019-nCoV", "virus", "contact",
            "transmission", "infection", "confirmed", "case", "susceptible", "suspected",
            "epidemic", "morbidity", "isolation", "NAT"]

print("Tracking: " + str(Words_UK))
#  here we ste the listener object
listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True))
streamer = tweepy.Stream(auth=auth, listener=listener)
streamer.filter(locations=Loc_UK, languages=['en'], track=Words_UK,
                is_async=True)  # locations= Loc_UK, track = Words_UK, track=Words_UK
#  the following line is for pure 1% sample
# we can only use filter or sample - not both together
# streamer.sample(languages = ['en'])

Place = 'London'
Lat = '51.450798'
Long = '-0.137842'
geoTerm = Lat + ',' + Long + ',' + '1000km'
#

last_id = None
counter = 0
sinceID = None
results = True
'''
q="Boris" or "Prime Minister" or "coronavirus" or "2019-nCoV" or "virus" or "contact" or
                      "transmission" or "infection" or "confirmed" or "case" or "susceptible" or "suspected" or
                      "epidemic" or "morbidity" or "isolation" or "NAT"
q="Boris" or "Prime Minister" or "independence" or "European Union" or "EU" or "Scottish voters" or
                      "departure" or "Scotland" or "politics" or "referendum" or "Brexit" or "economic" or
                      "economy"                    
'''

while results:
    # print(geoTerm)
    if counter < 50000:
        try:

            results = api.search(
                q="Boris" or "Prime Minister" or "independence" or "European Union" or "EU" or "Scottish voters" or
                  "departure" or "Scotland" or "politics" or "referendum" or "Brexit" or "economic" or
                  "economy", geocode=geoTerm, count=10000, max_id=last_id, lang="en",
                tweet_mode='extended')  # until='2021-03-22',
            for x in results:
                # print("11111111/n", x)
                resultsJson = x._json
                tweet2 = processTweets1(resultsJson)
                print("REST api:", tweet2)
                try:
                    collection1.insert_one(tweet2)
                    with open("./original_data/rest.json", "a") as f:
                        f.writelines(json_util.dumps(tweet2))
                        f.write('\n')
                        print("Loading ini the file ...")
                except Exception as e:
                    print(e)
        except Exception as e:
            print(e)
            counter += 1
    else:
        # the following let the crawler to sleep for 15 minutes; to meet the Twitter 15 minute restriction
        time.sleep(15 * 60)


