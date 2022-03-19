import json
import time
import math

with open('textGrouped.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

cout0 = 0
cout = 0
data_set = []
mark_set = []
text_set = []
follower_set = []
age_set = []
verified_set = []
profile_set = []
avgClass = []

for i in range(0, len(data)):
    groupClass = []
    sumVal = 0
    for j in range(0, len(data[i])):
        groupCol = []
        followerVal = data[i][j][5]
        veridiedVal = data[i][j][6]
        profileVal = data[i][j][7]
        descriptionVal = data[i][j][8]
        timeArry = time.strptime(data[i][j][4], "%a %b %d %H:%M:%S +0000 %Y")
        ageTime = time.time() - time.mktime(timeArry)
        ageVal = int(abs(ageTime)) // 24 // 3600

        dayMark = 0
        followerMark = 0
        verifiedMark = 0
        profileMark = 0
        descriptionMark = 0

        if ageVal < 1:
            dayMark = 0.05
        elif ageVal < 30:
            dayMark = 0.10
        elif ageVal > 90:
            dayMark = 0.25

        if followerVal < 50:
            followerMark = 0.5
        elif followerVal < 5000:
            followerMark = 1.0
        elif followerVal < 10000:
            followerMark = 1.5
        elif followerVal < 100000:
            followerMark = 2.0
        elif followerVal < 200000:
            followerMark = 2.5
        elif followerVal > 200000:
            followerMark = 3.0

        if veridiedVal:
            verifiedMark = 1.5
        else:
            verifiedMark = 1.0

        if profileVal:
            profileMark = 0.5
        else:
            profileMark = 1

        if descriptionVal:
            descriptionMark = 1.2
        else:
            descriptionMark = 0.8

        qualityScore = (dayMark + verifiedMark + followerMark + profileMark + descriptionMark) / 5
        # print(ageTime)
        # groupCol.append(ageTime)
        groupClass.append(qualityScore)
        sumVal += qualityScore

        # x = computeScore(data[i][j][3], data[i][j][4], data[i][j][5], data[i][j][6])
    mark_set.append(groupClass)
    avgClass.append(sumVal / len(data[i]))

# print(mark_set)

for x in range(0, len(mark_set)):
    for i in range(0, len(mark_set[x])):
        if len(mark_set[x]) > 1:
            for j in range(i + 1, len(mark_set[x])):
                if mark_set[x][i] > mark_set[x][j]:
                    maxVal = mark_set[x][i]
                    mark_set[x][i] = mark_set[x][j]
                    mark_set[x][j] = maxVal
                    maxSet = data[x][j]
                    data[x][i] = data[x][j]
                    data[x][j] = maxSet

# print(mark_set)

for x in range(0, len(mark_set)):
    for i in range(x + 1, len(mark_set)):
        if avgClass[x] > avgClass[i]:
            turn1 = avgClass[x]
            avgClass[x] = avgClass[i]
            avgClass[i] = turn1

            turn2 = mark_set[x]
            mark_set[x] = mark_set[i]
            mark_set[i] = turn2

            turn3 = data[x]
            data[x] = data[i]
            data[i] = turn3

# print(mark_set)
# print("---------------------")
# print(avgClass)

dataFilter = []

for x in data:
    if len(x) > 10:
        dataFilter.append(x)

minSize = len(dataFilter[0])
maxSize = 0
avgSize = 0
for x in dataFilter:
    if len(x) < minSize:
        minSize = len(x)
    if len(x) > maxSize:
        maxSize = len(x)
    avgSize += len(x)

print("Arrange the data within and between groups in ascending order, and delete groups of less than 10 items as noise")
print("total nums group is :", len(dataFilter))
print("filtered group max Size:", maxSize)
print("filtered group min Size:", minSize)
print("filtered group average Size:", avgSize / len(dataFilter))

with open("textRanked.json", 'w', encoding="utf-8") as g1:
    json.dump(dataFilter, g1, ensure_ascii=False)


def computeTF(wordDict, bow):
    tfDict = {}
    bowCount = len(bow)
    for word, count in wordDict.items():
        tfDict[word] = count / float(bowCount)
    return tfDict


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


def computeTFIDF(tfBow, idfs, cId):
    tfidf = []
    for word, val in tfBow.items():
        tfidfInfo = []
        tfidfInfo.append(word)
        tfidfInfo.append([val * idfs[word]])
        tfidf.append(tfidfInfo)
    return tfidf


print("For valuable information, select the top 5 TF-IDF words in each group")
for group1 in dataFilter:
    wordSet = []
    wordSet1 = []
    print(group1)
    group = group1[0:10]
    for tweetContent in group:
        wordSet = list(set(wordSet).union(set(tweetContent[1])))
    for i in range(0, len(group)):
        wordSet1.append(dict.fromkeys(wordSet, 0))
    for sentence in range(0, len(group)):
        for word in group[sentence][1]:
            wordSet1[sentence][word] += 1

    tfresult = []

    for x in range(0, len(group)):
        x = computeTF(wordSet1[x], group[x][1])
        tfresult.append(x)

    idfresult = computeIDF(wordSet1)

    tfidfResult = []

    worthResult = []
    for x in range(0, len(group)):
        worthGroup = computeTFIDF(tfresult[x], idfresult, x)
        worthGroupInfo = []
        for i in range(0, len(worthGroup)):
            if worthGroup[i][1][0] != 0:
                columGroup = []
                columGroup.append(worthGroup[i][0])
                columGroup.append(worthGroup[i][1])
                worthGroupInfo.append(columGroup)
        worthResult.append(worthGroupInfo)
    groupKey = []
    for x in worthResult:
        groupKeyInfo = []
        count10 = 0
        for i in x:
            if len(i) > 0 & count10 < 3:
                groupKeyInfo.append(i[0])
                groupKeyInfo.append(i[1])
            count10 += 1
        groupKey.append(groupKeyInfo)
    try:
        groupKey.sort(key=lambda sublist: sublist[0][1], reverse=True)
        resultTop = groupKey[0:5]
        for x in resultTop:
            print(x[0])
    except Exception as e:
        pass
