from pymongo import MongoClient
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer
# from stop_words import get_stop_words
# import time
import json

with open('./stopwords.txt', 'rb') as f:
    STOP_WORDS = [line.strip() for line in f.readlines()]

client = MongoClient('127.0.0.1', 27017)  # is assigned local port
dbName = "TwitterDump"  # set-up a MongoDatabase
db = client[dbName]
collName1 = 'colTest1'  # here we create a collection
collName2 = 'colTest2'
collName3 = 'colTest3'
collName4 = 'colTest4'
collName5 = 'colTest5'
collName6 = 'colTest6'
collection1 = db[collName1]  # This is for the Collection  put in the DB
collection2 = db[collName2]
collection3 = db[collName3]
collection4 = db[collName4]  # This is for the Collection  put in the DB
collection5 = db[collName5]
collection6 = db[collName6]
bol1 = False
result = collection4.find()

words = stopwords.words('english')
print(words)
for w in ['!', ',', '.', '?', 'oh', 'got']:
    words.append(w)

tokenizer = RegexpTokenizer(r'\w+')
for x in result:
    raw = x['text'].lower()
    tokens = tokenizer.tokenize(raw)
    text = [x['text'] for x['text'] in tokens if not x['text'] in words]
    print(text)
    text = str(' '.join(text))

    id = (x['_id'])
    created = x['date']
    hashtags = x['hashtags']
    use_followers_count = x['followers_count']
    use_retweet_count = x['retweet_count']
    mList = x['mentions']
    hList = x['hashtags']
    tweet = {"created_at": created, "user": {"id": id, "followers_count": use_followers_count}, "text": text,
             "retweet_count": use_retweet_count, "entities": {"hashtags": hList, "user_mentions": mList}}
    try:
        with open("./data/cleaned_tweets/2021-03-25/3.json", "a") as f:
            f.writelines(json.dumps(tweet))
            f.write('\n')
            print("Loading ini the file ...")
    except Exception as e:
        print(e)
