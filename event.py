# import datetime
import time
import json
from nltk.tokenize import RegexpTokenizer
from stop_words import get_stop_words
import re
import math
import numpy as np

doc_set = []
bol = False
count = 0

time_set = []
id_set = []
geo_set = []
location_set = []
noPlace_set = []
count21 = 0
with open('original_data.json', 'r', encoding='utf-8')as f:
    try:
        while True:
            line = f.readline()
            if line:
                d = json.loads(line)
                if not d["retweet"]:
                    doc_set.append(d['text'])
                    time_set.append(d['date'])
                    id_set.append(d['_id'])
                    geo_set.append(d['coordinates'])
                    location_set.append(d['location'])
            else:
                break
    except Exception as e:
        print(e)
print("start Execute a tfidf process:")
eTimeStart = time.time()

tokenizer = RegexpTokenizer(r'\w+')
en_stop = get_stop_words('en')

k = doc_set
rekc = []

text1 = []

for i in k:
    raw = i.lower()
    raw = re.sub(r'(https|http)?:\/\/(\w|\.|\/|\?|\=|\&|\%)*\b', '', raw, flags=re.MULTILINE)
    # tokens = raw.split(" ")
    tokens = tokenizer.tokenize(raw)
    stopped_tokens = [i for i in tokens if not i in en_stop]
    if len(stopped_tokens) == 0:
        stopped_tokens = 'emptytweet'
    # print(stopped_tokens)
    text1.append(stopped_tokens)

wordSet = []
wordSet1 = []


def cosineSim(sentence1, sentence2):
    seg1 = sentence1
    seg2 = sentence2
    word_list = list(set([word for word in seg1 + seg2]))
    word_count_vec_1 = []
    word_count_vec_2 = []
    for word in word_list:
        word_count_vec_1.append(seg1.count(word))
        word_count_vec_2.append(seg2.count(word))

    vec_1 = np.array(word_count_vec_1)
    vec_2 = np.array(word_count_vec_2)

    num = vec_1.dot(vec_2.T)
    denom = np.linalg.norm(vec_1) * np.linalg.norm(vec_2)
    cos = num / denom
    sim = 0.5 + 0.5 * cos

    return sim


for i in text1:
    wordSet = list(set(wordSet).union(set(i)))
for i in range(0, len(text1)):
    wordSet1.append(dict.fromkeys(wordSet, 0))

for sentence in range(0, len(text1)):
    for word in text1[sentence]:
        wordSet1[sentence][word] += 1


def computeTF(wordDict, bow):
    tfDict = {}
    bowCount = len(bow)
    for word, count in wordDict.items():
        tfDict[word] = count / float(bowCount)
    return tfDict


tfresult = []

for x in range(0, len(text1)):
    x = computeTF(wordSet1[x], text1[x])
    tfresult.append(x)


def computeIDF(docList):
    idfDict = {}
    N = len(docList)

    idfDict = dict.fromkeys(docList[0].keys(), 0)
    for doc in docList:
        for word, val in doc.items():
            if val > 0:
                idfDict[word] += 1

    for word, val in idfDict.items():
        idfDict[word] = math.log10(N / float(val))

    return idfDict


idfresult = computeIDF(wordSet1)


def computeTFIDF(tfBow, idfs, cId):
    tfidf = []
    for word, val in tfBow.items():
        tfidfInfo = []
        tfidfInfo.append(word)
        tfidfInfo.append([val * idfs[word], id_set[cId], time_set[cId], geo_set[cId], location_set[cId]])
        tfidf.append(tfidfInfo)
    return tfidf


tfidfResult = []

# for x in range(0, len(text1)):
#     tfidfResult.append(computeTFIDF(tfresult[x], idfresult, x))
tfidfTime = time.time()
print("tf-idf spent time %.2f sec" % (tfidfTime - eTimeStart))
print("starting events find......")

worthResult = []
for x in range(0, len(text1)):
    worthGroup = computeTFIDF(tfresult[x], idfresult, x)
    worthGroupInfo = []
    for i in range(0, len(worthGroup)):
        if worthGroup[i][1][0] != 0:
            columGroup = []
            columGroup.append(worthGroup[i][0])
            columGroup.append(worthGroup[i][1])
            worthGroupInfo.append(columGroup)
    worthResult.append(worthGroupInfo)

termTweets = []
count1 = 0
maktag = 0

count9 = 0
loopTimes = 0
count10 = 0

specicaial = []

# Calculation of events
for x in worthResult:
    topV = []
    # Fetch the TF-IDF values of the top 10
    x.sort(key=lambda sublist: sublist[1][0], reverse=True)
    if len(x) <= 10:
        for i in x:
            topV.append(i)
    else:
        topV = x[0:10]
    # Put the first data directly store in
    if count1 == 0:
        timeMoment = time.mktime(time.strptime(topV[0][1][2], "%a %b %d %H:%M:%S +0000 %Y"))
        termTweets.append([topV[0][0], [topV], timeMoment, count1])
        count1 += 1
    else:
        setV1 = []
        # Get the latest 10 events generating inverted table
        termTweets.sort(key=lambda sublist: sublist[2], reverse=True)
        marke = 0
        for nsh in termTweets:
            nsh[3] = marke
            marke += 1
        kid = 0
        if len(termTweets) < 10:
            for ki in termTweets:
                setV1.append(ki)
        else:
            setV1 = termTweets[0:10]
        # Compare to the inverted table to find sim result by cosine similarity
        textA = []
        for i in topV:
            textA.append(i[0])

        maxSim = 0
        count3 = 0
        for j in range(0, len(setV1)):
            for i in setV1[j][1]:
                for imp in i:
                    textB = []
                    for textlen in imp:
                        textB.append(textlen[0])
                    tempSim = cosineSim(textA, textB)
                    if tempSim > maxSim:
                        maxSim = tempSim
            count3 += 1

        # threshold  0.6. based on the findings from the related works
        # (McMinn, Jose, 2015, Petrović, Osborne, Lavrenko, 2010)
        if maxSim > 0.6:
            setV2 = []
            qid = 0
            # mark1 = []
            # The sequence of similar events is first found, and then the latest event is extracted
            # from the result of the event to make up to top-10
            for j in topV:
                count4 = 0
                for i in termTweets:
                    for k in i[1]:
                        for n in k:
                            for m in n:
                                if j[0] == m[0]:
                                    setV2.append(i)
                                    # mark1.append(count4)
                                    break
                    count4 += 1
            if len(setV2) < 10:
                if len(termTweets) + len(setV2) > 10:
                    for ki in termTweets[0:(10 - len(setV2))]:
                        setV2.append(ki)
                else:
                    for kj in termTweets:
                        setV2.append(kj)
            else:
                setV2 = setV2[0:10]
            # Sets the collection of shards events used to merge similar events
            fragVector = []
            maxSim1 = 0
            count2 = 0
            for j in setV2:
                avgSim1 = 0
                for ils in j[1]:
                    if len(j[1]) > 0:
                        textC = []
                        for imp1 in ils:
                            for imp2 in imp1:
                                textC.append(imp2[0])
                        avgSim1 += cosineSim(textA, textC)
                    print(len(j[1]))
                if len(j[1]) > 0:
                    tempSim1 = avgSim1 / len(j[1])

                # Calculate the centroid of mass
                if avgSim1 > maxSim1:
                    maxSim1 = avgSim1
                    kid = j[3]
                # thresold for shards events
                if tempSim1 >= (0.6 + 0.07):
                    fragVector.append(j)
                count2 += 1

            if maxSim1 > 0.6:
                # Treatment after exceeding the maximum Q value
                if len(termTweets[kid][1]) > 24:
                    minTimeMark = termTweets[kid][2]
                    specicaial.append(len(termTweets[kid][1]))
                    specicaial.append(termTweets[kid][1])
                    markid = 0
                    count10 += 1
                    # Find the earliest event and delete it
                    for y in termTweets:
                        if y[3] == kid:
                            for ny in y[1]:
                                count6 = 0
                                count7 = 0
                                for ny1 in ny[0]:
                                    if count6 == 1:
                                        count5 = 0
                                        for ny2 in ny1:
                                            if count5 == 1:
                                                tempTime = time.mktime(
                                                    time.strptime(ny2[2], "%a %b %d %H:%M:%S +0000 %Y"))
                                                if tempTime < minTimeMark:
                                                    minTimeMark = tempTime
                                                    markid = count7
                                                break
                                            count5 += 1
                                        count6 += 1
                                        break
                                count7 += 1

                    termTweets[kid][1][markid] = [topV]
                    avgTime = 0

                    # Recalculate the average time
                    for y in termTweets:
                        if y[3] == kid:
                            for ny in y[1]:
                                count6 = 0
                                for ny1 in ny[0]:
                                    if count6 == 1:
                                        count5 = 0
                                        for ny2 in ny1:
                                            if count5 == 1:
                                                avgTime += time.mktime(
                                                    time.strptime(ny2[2], "%a %b %d %H:%M:%S +0000 %Y"))
                                                break
                                            count5 += 1
                                        count6 += 1
                                        break
                    termTweets[kid][2] = int(avgTime / len(termTweets[kid][1]))

                    # Merge the fragmentation events
                    if len(fragVector) > 1:
                        groupFragNum = []
                        groupFragUn = []
                        for fk in range(0, len(fragVector)):
                            for fk1 in range(fk + 1, len(fragVector)):
                                groupFragUn = fragVector[fk][1] + fragVector[fk1][1]
                                anum = fragVector[fk1][3]
                                del termTweets[anum:anum + 1]
                        bnum = fragVector[0][3]
                        termTweets[bnum][1] = groupFragUn[0:int(len(groupFragUn) / len(fragVector))]
                        maktag = 0
                else:
                    # Adds new classes to similar event clusters
                    termTweets[kid][1].append([x])
                    avgTime1 = 0
                    count9 += 1
                    for y in termTweets:
                        if y[3] == kid:
                            for ny in y[1]:
                                count6 = 0
                                for ny1 in ny[0]:
                                    if count6 < 1:
                                        count5 = 0
                                        for ny2 in ny1:
                                            if count5 == 1:
                                                avgTime1 += time.mktime(
                                                    time.strptime(ny2[2], "%a %b %d %H:%M:%S +0000 %Y"))
                                                break
                                            count5 += 1
                                        count6 += 1
                                        break

                    termTweets[kid][2] = int(avgTime1 / len(termTweets[kid][1]))
            else:
                # Delete the earliest event after exceeding the table length E
                if (len(termTweets) <= 25) & (maktag == 0):
                    timeMoment4 = time.mktime(time.strptime(topV[0][1][2], "%a %b %d %H:%M:%S +0000 %Y"))
                    termTweets.append([topV[0][0], [[topV]], timeMoment4, count1])
                    count1 += 1
                    if len(termTweets) == 25:
                        maktag += 1

                elif maktag > 0:
                    avgLen1 = 0
                    for item in termTweets:
                        avgLen1 += len(item[1])
                    avgLen1 = avgLen1 / len(termTweets)
                    # print("avglen:", avgLen1)
                    minLen2 = avgLen1
                    for iem in termTweets:
                        if len(iem[1]) < minLen2:
                            minLen2 = len(iem[1])
                    termTweets.sort(key=lambda sublist: sublist[2], reverse=True)
                    for item1 in termTweets:
                        if len(item1[1]) == minLen2:
                            timeMoment3 = time.mktime(time.strptime(topV[0][1][2], "%a %b %d %H:%M:%S +0000 %Y"))
                            termTweets[0] = [topV[0][0], [[topV]], timeMoment3, 0]
                            break
        # Delete the earliest event after exceeding the table length E
        else:
            if (len(termTweets) <= 25) & (maktag == 0):
                timeMoment2 = time.mktime(time.strptime(topV[0][1][2], "%a %b %d %H:%M:%S +0000 %Y"))
                termTweets.append([topV[0][0], [[topV]], timeMoment2, count1])
                count1 += 1
                if len(termTweets) == 25:
                    maktag += 1

            elif maktag > 0:
                avgLen = 0
                for item in termTweets:
                    avgLen += len(item[1])
                avgLen = avgLen / len(termTweets)
                # print("avglen:", avgLen)
                minLen1 = avgLen
                for iem in termTweets:
                    if len(iem[1]) < minLen1:
                        minLen1 = len(iem[1])
                termTweets.sort(key=lambda sublist: sublist[2], reverse=True)
                for item1 in termTweets:
                    if len(item1[1]) == minLen1:
                        timeMoment2 = time.mktime(time.strptime(topV[0][1][2], "%a %b %d %H:%M:%S +0000 %Y"))
                        termTweets[0] = [topV[0][0], [[topV]], timeMoment2, 0]
                        break

    loopTimes += 1

for x in termTweets:
    print(x)
    print(len(x[1]))
# print(len(termTweets))
# print(count9)
# print(count10)
# print(specicaial)
finalR = []
count22 = 0
count23 = 0
count24 = 0
count26 = 0
for x in termTweets:
    if len(x[1]) > 10:
        count26 += 1
        groupFinalR = []
        for i in x[1]:
            for j in i:
                groupFinalR.append(j[0][1][2])
                count25 = 0
                for xn in j:
                    if count25 < 1:
                        if xn[1][3]:
                            count22 += 1
                        if xn[1][4]:
                            count23 += 1
                        if (xn[1][3] is None) & (xn[1][4] is None):
                            count24 += 1
                    count25 += 1
        finalR.append(groupFinalR)
print("total clusters:", len(doc_set))
print("total time spent %.2f sec" % (time.time() - eTimeStart))
print("geo-coordination tag:", count22)
print("location tag:", count23)
print("without any geo tag tag:", count24)
print("events num:", count26)
