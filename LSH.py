from pymongo import MongoClient
from nltk.tokenize import RegexpTokenizer
from stop_words import get_stop_words
import sys
import random
import time
import binascii
import numpy as np
import re
import json

# client = MongoClient('127.0.0.1', 27017)  # is assigned local port
# dbName = "TwitterDump"  # set-up a MongoDatabase
# db = client[dbName]
# collName = 'colTest3'  # here we create a collection
# collection = db[collName]  # This is for the Collection  put in the DB

numHashes = 300  # The length of the signature matrix

processStart = time.time()
doc_set = []
bol = False
count3 = 0
# result = collection.find({"retweet": bol}, {"text": 1, "uid": 1, "_id": 1})
id_set = []

with open('./LSHdata/LSHdata.json', 'r', encoding='utf-8') as f:
    data1 = json.load(f)

for i in range(0, len(data1)):
    if not data1[i]['retweet']:
        doc_set.append(data1[i]['text'])
        id_set.append(data1[i]['_id'])
    count3 += 1

# for x in result:
#     doc_set.append(x['text'])
#     id_set.append(x['_id'])
#     count3 += 1

# print(doc_set)
numDocs = len(doc_set)

curShingleID = 0

docsAsShingleSets = {}

t0 = time.time()

totalShingles = 0
docNames = []
text = []

tokenizer = RegexpTokenizer(r'\w+')
en_stop = get_stop_words('en')

for i in doc_set:
    raw = i.lower()
    raw = re.sub(r'(https|http)?:\/\/(\w|\.|\/|\?|\=|\&|\%)*\b', '', raw, flags=re.MULTILINE)
    tokens = tokenizer.tokenize(raw)
    stopped_tokens = [i for i in tokens if not i in en_stop]
    text.append(stopped_tokens)

for i in range(0, numDocs):
    words = text[i]
    shinglesInDoc = set()
    docNames.append(id_set[i])
    for index in range(0, len(words) - 2):
        shingle = words[index] + " " + words[index + 1] + " " + words[index + 2]

        crc = binascii.crc32(shingle.encode())
        shinglesInDoc.add(crc)

    docsAsShingleSets[id_set[i]] = shinglesInDoc
    totalShingles = totalShingles + (len(words) - 2)

print("total tweets:", count3)

print('\nRemoving retweets and Shingling ' + str(numDocs) + ' docs took %.2f sec.' % (time.time() - t0))

print('\nAverage shingles per doc: %.2f' % (totalShingles / numDocs))


# numElems = int(numDocs * (numDocs - 1) / 2)

# estJSim = [0 for x in range(numElems)]

def getTriangleIndex(i, j):
    # If i == j that's an error.
    if i == j:
        sys.stderr.write("Can't access triangle matrix with i == j")
        sys.exit(1)
    # If j < i just swap the values.
    if j < i:
        temp = i
        i = j
        j = temp
    k = int(i * (1000 - (i + 1) / 2.0) + j - i) - 1

    return k


t0 = time.time()

print('\nGenerating random hash functions...')

maxShingleID = 2 ** 32 - 1
nextPrime = 4294967311


def pickRandomCoeffs(k):
    # Create a list of 'k' random values.
    randList = []

    while k > 0:
        # Get a random shingle ID.
        randIndex = random.randint(0, maxShingleID)

        # Ensure that each random number is unique.
        while randIndex in randList:
            randIndex = random.randint(0, maxShingleID)

            # Add the random number to the list.
        randList.append(randIndex)
        k = k - 1

    return randList


coeffA = pickRandomCoeffs(numHashes)
coeffB = pickRandomCoeffs(numHashes)

print('\nGenerating MinHash signatures for all documents...')

signatures = []

for docID in docNames:
    shingleIDSet = docsAsShingleSets[docID]
    signature = []
    for i in range(0, numHashes):

        # For each of the shingles actually in the document, calculate its hash code
        # using hash function 'i'.

        # Track the lowest hash ID seen. Initialize 'minHashCode' to be greater than
        # the maximum possible value output by the hash.
        minHashCode = nextPrime + 1

        # For each shingle in the document...
        for shingleID in shingleIDSet:
            # Evaluate the hash function.
            hashCode = (coeffA[i] * shingleID + coeffB[i]) % nextPrime

            # Track the lowest hash code seen.
            if hashCode < minHashCode:
                minHashCode = hashCode

        # Add the smallest hash code value as component number 'i' of the signature.
        signature.append(minHashCode)

    # Store the MinHash signature for this document.
    signatures.append(signature)

elapsed = (time.time() - t0)

print("\nGenerating MinHash signatures took %.2fsec" % elapsed)

print('\nComparing all signatures...')

t0 = time.time()

signaturesT = [[row[col] for row in signatures] for col in range(len(signatures[0]))]

# For each of the test documents...
buckt = set()
count1 = 0
b = 50  # each band b
r = 2000  # each doc r
threshold = 0.7  # limit the compare num

mark = 0
mark1 = 0

group = np.array(signatures)

numElems = int(r * (r - 1) / 2)

clusterList = []


# The signature matrix is divided into N buckets according to a band for each b and a doc for each r
def Matrix_block(matrix, Row, Col):
    a = matrix.shape[0]
    print(a)
    b = matrix.shape[1]
    count4 = 0
    mark = 0
    count6 = 0
    for row in range(int(a / Row)):
        for col in range(int(b / Col)):
            group2 = matrix[row * Row:(row + 1) * Row, col * Col:(col + 1) * Col]
            count4 += 1
            # Each bucket is considered to be equal if there are hash values that are equal over a certain threshold 0.4
            for i in range(0, len(group2[0])):
                signature1 = group2[i]
                for j in range(i + 1, len(group2[0])):
                    signature2 = group2[j]
                    count5 = 0
                    for k in range(0, Col):
                        count5 += (signature1[k] == signature2[k])
                    if count5 / Col > threshold:
                        buckt.add(docNames[count6 + i] + " " + docNames[count6 + j])
    count6 += Row


t1 = time.time()

x = Matrix_block(group, r, b)

print("bucket:", buckt)
print("there are ", len(buckt), "pair")
print("Comparative similar time: %.2fsec" % (time.time() - t1))

arrayResult = []
for x in buckt:
    k = x.split(' ')
    numInfo = {k[0], k[1]}
    arrayResult.append(numInfo)

lenth = len(arrayResult)

for i in range(1, lenth):
    for j in range(i):
        if arrayResult[i] == {0} or arrayResult[j] == {0}:
            continue
        x = arrayResult[i].union(arrayResult[j])
        y = len(arrayResult[i]) + len(arrayResult[j])
        if len(x) < y:
            arrayResult[i] = x
            arrayResult[j] = {0}
clusters = [i for i in arrayResult if i != {0}]
minC = len(clusters)
maxC = 0
avgC = 0
for x in clusters:
    if len(x) < minC:
        minC = len(x)
    if len(x) > maxC:
        maxC = len(x)
    avgC += len(x)
print("Total have ", len(clusters), "clusters")
print("average cluster:", avgC / len(clusters))
print("minimum cluster:", minC)
print("maximum cluster:", maxC)
print("total time spent: %.2f sec" % (time.time() - processStart))
