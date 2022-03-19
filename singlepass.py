import time

from pymongo import MongoClient

import math
import json
import re

from nltk.tokenize import RegexpTokenizer
from stop_words import get_stop_words
from nltk.stem.porter import PorterStemmer

b = time.time()

with open('./singlepass_data/singlepassdata.json', 'r', encoding='utf-8') as f:
    data1 = json.load(f)
doc_set = []
time_set = []
id_set = []
accountAge_set = []
followers_set = []
verified_set = []
profile_set = []
description_set = []
hashtags_set = []
mention_set = []
for x in data1:
    doc_set.append(x['text'])
    time_set.append(x['date'])
    id_set.append(x['_id'])
    accountAge_set.append(x['user_time'])
    followers_set.append(x['followers_count'])
    verified_set.append(x['verified'])
    profile_set.append(x['user_profile'])
    description_set.append(x['description'])
    hashtags_set.append(x['hashtags'])
    mention_set.append(x['mentions'])


class clustery(object):
    clusterList = []

    def __init__(self):
        self.clusterCounter = 0

    def createCluster(self, id, text, time, tweetAge, tweetFollowers
                      , tweetVerified, tweetProfile, tweetDescription, tweetDoc, tweetHashtags, tweetMentions):
        clusterInfo = [id, text, time, self.clusterCounter, tweetAge, tweetFollowers, tweetVerified, tweetProfile,
                       tweetDescription, tweetDoc, tweetHashtags, tweetMentions]
        groupClusterCom = []
        groupClusterCom.append(clusterInfo)
        self.clusterList.append(groupClusterCom)

    def getCluster(self, gcid, gctext, gctime, gcage, gcfollower, gcverified, gcprofile, gcdescription, gcdoc,
                   gchashtags, gcmentions):
        maxSim = 0
        sim = 0
        cid = 0

        tweetId = gcid
        tweetText = gctext
        tweetTime = gctime
        tweetAge = gcage
        tweetFollowers = gcfollower
        tweetVerified = gcverified
        tweetProfile = gcprofile
        tweetDescription = gcdescription
        tweetDoc = gcdoc
        tweetHashtags = gchashtags
        tweetMentions = gcmentions

        if self.clusterCounter == 0:
            self.createCluster(tweetId, tweetText, tweetTime, tweetAge, tweetFollowers
                               , tweetVerified, tweetProfile, tweetDescription, tweetDoc, tweetHashtags, tweetMentions)
            self.clusterCounter += 1
        else:
            # try:
            for cluster1 in range(0, len(self.clusterList)):
                sim = self.computeSimilarity(tweetText, cluster1)
                if (sim > maxSim):
                    maxSim = sim
                    cid = cluster1

            if (maxSim < 0.6):
                self.createCluster(tweetId, tweetText, tweetTime, tweetAge, tweetFollowers
                                   , tweetVerified, tweetProfile, tweetDescription, tweetDoc, tweetHashtags,
                                   tweetMentions)
                self.clusterCounter += 1
            else:
                self.addCluster(tweetId, tweetText, tweetTime, cid, tweetAge, tweetFollowers
                                , tweetVerified, tweetProfile, tweetDescription, tweetDoc, tweetHashtags, tweetMentions)
        # except Exception as e:
        #     print(e)
        return

    def computeSimilarity(self, text_ori, sid):
        avgSim = 0
        coText = text_ori
        for x in self.clusterList[sid]:
            avgSim += self.cosineSim(x[1], coText)

        return avgSim / len(self.clusterList[sid])

    def cosineSim(self, text_a, text_b):
        words1 = text_a
        words2 = text_b
        # print(words1)
        words1_dict = {}
        words2_dict = {}
        for word in words1:
            if word != '' and (word in words1_dict):
                num = words1_dict[word]
                words1_dict[word] = num + 1
            elif word != '':
                words1_dict[word] = 1
            else:
                continue
        for word in words2:
            if word != '' and (word in words2_dict):
                num = words2_dict[word]
                words2_dict[word] = num + 1
            elif word != '':
                words2_dict[word] = 1
            else:
                continue
        dic1 = sorted(words1_dict.items(), key=lambda asd: asd[1], reverse=True)
        dic2 = sorted(words2_dict.items(), key=lambda asd: asd[1], reverse=True)

        # Get word vector
        words_key = []
        for i in range(len(dic1)):
            words_key.append(dic1[i][0])  # Adds an element to an array
        for i in range(len(dic2)):
            if dic2[i][0] in words_key:
                # print 'has_key', dic2[i][0]
                pass
            else:  # merge
                words_key.append(dic2[i][0])
        vect1 = []
        vect2 = []
        for word in words_key:
            if (word in words1_dict):
                vect1.append(words1_dict[word])
            else:
                vect1.append(0)
            if (word in words2_dict):
                vect2.append(words2_dict[word])
            else:
                vect2.append(0)

        # Calculate the cosine similarity
        sum = 0
        sq1 = 0
        sq2 = 0
        for i in range(len(vect1)):
            sum += vect1[i] * vect2[i]
            sq1 += pow(vect1[i], 2)
            sq2 += pow(vect2[i], 2)
        try:
            result = round(float(sum) / (math.sqrt(sq1) * math.sqrt(sq2)), 2)
        except ZeroDivisionError:
            result = 0.0
        return result

    def addCluster(self, uid, text, time, cid, tweetAge, tweetFollowers
                   , tweetVerified, tweetProfile, tweetDescription, tweetDoc, tweetHashtags, tweetMentions):
        clusterInfo = [uid, text, time, cid, tweetAge, tweetFollowers, tweetVerified, tweetProfile, tweetDescription,
                       tweetDoc, tweetHashtags, tweetMentions]
        self.clusterList[cid].append(clusterInfo)


# with open("textTestData.json", 'w', encoding="utf-8") as g2:
#     json.dump(doc_set, g2, ensure_ascii=False)


tokenizer = RegexpTokenizer(r'\w+')
en_stop = get_stop_words('en')
text = []

# print(time_set)
print("Start singlepass....")
for i in doc_set:
    raw = i.lower()
    raw = re.sub(r'(https|http)?:\/\/(\w|\.|\/|\?|\=|\&|\%)*\b', '', raw, flags=re.MULTILINE)
    tokens = tokenizer.tokenize(raw)
    stopped_tokens = [i for i in tokens if not i in en_stop]
    text.append(stopped_tokens)

firstTime = time.time()
c = firstTime - b
# print(text)
clusterText = clustery()

for i in range(0, len(text)):
    clusterText.getCluster(id_set[i], text[i], time_set[i], accountAge_set[i]
                           , followers_set[i], verified_set[i], profile_set[i]
                           , description_set[i], doc_set[i], hashtags_set[i], mention_set[i])

secondTime = time.time()
print(clusterText.clusterList)
print("total cluster:", clusterText.clusterCounter)

count1 = 0
for x in clusterText.clusterList:
    for i in range(0, len(x)):
        count1 += 1

print("max cluster size", count1)
print("The time to read the data: %.2f sec" % c)
print("The time to cluster 2000 similar texts: %.2f sec" % (secondTime - firstTime))
startRankTime = time.time()

with open("textGrouped.json", 'w', encoding="utf-8") as g1:
    json.dump(clusterText.clusterList, g1, ensure_ascii=False)
