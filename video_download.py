import requests
import re
import json


def download_video(video_links):
    root = './video/'
    for link in video_links:
        file_name = link.split('/')[-1]
        file_name = re.sub(r'\?.*', '', file_name)
        print("download process: %s" % file_name)
        r = requests.get(link, stream=True).iter_content(chunk_size=1024 * 1024)
        with open(root + file_name, 'wb') as f:
            for chunk in r:
                if chunk:
                    f.write(chunk)

        print("%s complete!\n" % file_name)
    print("all finish!")
    return


if __name__ == "__main__":
    # this is to setup local Mongodb
    # client = MongoClient('127.0.0.1', 27017)  # is assigned local port
    # dbName = "TwitterDump"  # set-up a MongoDatabase
    # db = client[dbName]
    # collName1 = 'colTest2'
    # collection1 = db[collName1]
    #
    # result8 = collection1.find({"videoLinks": {'$ne': None}})
    videoLinks = []
    video_amount = 0
    with open('./data1/3.29.1_stream.json', 'r', encoding='utf-8')as f:
        while True:
            line = f.readline()
            # print(line)
            if line:
                d = json.loads(line)
                if d['videoLinks']:
                    videoLinks.append(d['videoLinks'])
                    video_amount += 1
    # video_links = collection1.find({"_id": {'$ne': None}}, {"videoLinks": 1, "_id": 0})
                print(video_amount)
                download_video(videoLinks)
