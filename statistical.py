# # from pymongo import MongoClient
import json
# import time
rest_id = []
stream_id = []
rest_amount = 0
rest_retweet = 0
rest_geo = 0
rest_quote = 0
rest_video = 0
rest_photo = 0
rest_place = 0
rest_gif = 0
rest_verified = 0
with open('./original_data/3.29.1_rest.json', 'r', encoding='utf-8')as f:
    try:
        while True:
            line = f.readline()
            if line:
                d = json.loads(line)
                rest_id.append(d['uid'])
                rest_amount += 1
                if d['retweet'] == True:
                    # print(d['retweet'])
                    rest_retweet += 1

                if d['geoenabled'] == True:
                    rest_geo += 1
                if d['location']:
                    rest_place += 1
                if d['verified'] == True:
                    rest_verified += 1
                if d['quote'] == True:
                    rest_quote += 1
            else:
                break
    except Exception as e:
        print(e)

stream_amount = 0
stream_retweet = 0
stream_geo = 0
stream_quote = 0
stream_video = 0
stream_photo = 0
stream_place = 0
stream_gif = 0
stream_verified = 0
with open('./original_data/3.29.1_stream.json', 'r', encoding='utf-8')as f:
    while True:
        line = f.readline()
        # print(line)
        if line:
            d = json.loads(line)
            stream_id.append(d['uid'])
            stream_amount += 1
            media = d['media']
            # print(media)
            if d['retweet'] == True:
                # print(d['retweet'])
                stream_retweet += 1
            if d['geoenabled'] == True:
                stream_geo += 1
            if d['location']:
                stream_place += 1
            if media == 'photo':
                stream_photo += 1
            if d['videoLinks']:
                stream_video += 1
            if media == 'animated_gif':
                stream_gif += 1
            if d['verified'] == True:
                stream_verified += 1
            if d['quote'] == True:
                stream_quote += 1
        else:
            break
# rest = []
# streaming = []
# for i in rest_id:
#     rest.append(i)
# for j in stream_id:
#     streaming.append(j)
count = 0
# print(len(rest_id))
# print(len(stream_id))
for i in stream_id:
    # print('i', i)
    # time.sleep(1)
    for j in rest_id:
        # print('j', j)
        if i == j:
            count = count + 1

print("total amounts：", stream_amount+rest_amount)
print('redundant data', count)
print("amounts of streaming:", stream_amount)
print("amounts of rest:", rest_amount)
print("tweets with geo-tag :", rest_geo + stream_geo)
print("tweets with locations/Place Object:", rest_place + stream_place)
print("Tweets with images:", stream_photo)
print("Tweets with videos:", stream_video)
print("tweets verified:", rest_verified + stream_verified)
print("tweets animated_gif:", stream_gif)
print("Tweets with retweets:", rest_retweet + stream_retweet)
print("Tweets with quotes:", rest_quote + stream_quote)

#
# # client = MongoClient('127.0.0.1', 27017)  # is assigned local port
# # dbName = "TwitterDump"  # set-up a MongoDatabase
# # db = client[dbName]
# # collName = 'colTest3'  # here we create a collection
# # collection = db[collName]  # This is for the Collection  put in the DB
# #
# # bol = True
# #
# # result0 = collection.find().count()
# # result1 = collection.find({"retweet": bol}).count()
# # result2 = collection.find({"quote": bol}).count()
# # result3 = collection.find({"media": "photo"}).count()
# # result4 = collection.find({"verified": bol}).count()
# # result5 = collection.find({"geoenabled": bol}).count()
# # result6 = collection.find({'$or': [{"location": {'$ne': None}}, {"place_name": {'$ne': None}}]}).count()
# # result7 = collection.find({'videoLinks': {'$ne': None}}).count()
# #
# # print("total amounts:", result0)
# # print("No of retweet:", result0-result1)
# # print("No of quote:", result0-result2)
# # print("No of photo:", result0-result3)
# # print("No of video:", result0-result7)
# # print("verified:", result4)
# # print("geo-tagged:", result5)
# # print("place or location:", result6)
# #
#
# with open('./data1/3.29_stream.json', 'r', encoding='utf-8')as f:
#     retweet = 0
#     total_amount = 0
#     geo = 0
#     quote = 0
#     video = 0
#     photo = 0
#     place = 0
#     gif = 0
#     verified = 0
#     while True:
#         line = f.readline()
#         # print(line)
#         if line:
#             d = json.loads(line)
#             total_amount += 1
#             media = d['media']
#             print(media)
#             if d['retweet'] == True:
#                 # print(d['retweet'])
#                 retweet += 1
#             if d['geoenabled'] == True:
#                 geo += 1
#             if d['location']:
#                 place += 1
#             if media == ['photo']:
#                 photo += 1
#             if media == 'video':
#                 video += 1
#             if media == 'animated_gif':
#                 gif += 1
#             if d['verified'] == True:
#                 verified += 1
#             if d['quote'] == True:
#                 quote += 1
#         else:
#             break
# print("total amounts:", total_amount)
# print("tweets with geo-tag :", geo)
# print("tweets with locations/Place Object:", place)
# print("Tweets with images:", photo)
# print("Tweets with videos:", video)
# print("tweets verified:", verified)
# print("tweets animated_gif:", gif)
# print("Tweets with retweets:", retweet)
# print("Tweets with quotes:", quote)


