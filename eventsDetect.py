# import argparse
from datetime import datetime
from DateInfo import DateInfo
from pymongo import MongoClient
# import time

client = MongoClient('127.0.0.1', 27017)  # is assigned local port
dbName = "TwitterDump"  # set-up a MongoDatabase
db = client[dbName]
collName = 'colTest1'  # here we create a collection
collection = db[collName]  # This is for the Collection  put in the DB

bol = False
result = collection.find({"retweet": bol})


class EventSummaryExtractor(object):
    def __init__(self):
        # self.DEF_INFILENAME = "ows.json"
        self.CATEGORIES = {}
        self.twittersdm = "%a %b %d %H:%M:%S Z %Y"
        self.dayhoursdm = "%Y-%b-%d:%H"
        self.daysdm = "%b/%d/%Y"
        self.hoursdm = "%H"

    def initialize_categories(self):
        self.CATEGORIES["People"] = ["protesters", "people"]
        self.CATEGORIES["Police"] = ["police", "cops", "nypd", "raid"]
        self.CATEGORIES["Media"] = ["press", "news", "media"]
        self.CATEGORIES["Location"] = ["nyc", "zucotti", "park"]
        self.CATEGORIES["Judiciary"] = ["judge", "eviction", "order", "court"]

    def extract_category_trends(self, fp):
        """
        :param filename:
        :return:
        """
        result = {}
        temp = ""
        catkeys = self.CATEGORIES.keys()
        datecount = {}

        # Open file and get time stamps from the tweets

        for temp in fp:
            d = ""
            # jobj = fp[3]
            # time = jobj[7]
            # d = datetime.fromtimestamp(time / 1000)
            # if "created_at" in jobj:
            #     time = ""
            #     time = jobj["created_at"]
            #     if not time:
            #         continue
            #     else:
            #         d = datetime.strptime(time, self.twittersdm)
            # elif "timestamp" in jobj:
            #     time = jobj["timestamp"]
            #     d = datetime.fromtimestamp(time / 1000)
            time = temp[7]
            d = datetime.fromtimestamp(time)
            datestr = d.strftime(self.dayhoursdm)
            text = temp[3]

            # Assign it to the category the tweets belong to
            for key in catkeys:
                words = self.CATEGORIES.keys()
                for word in words:
                    if word.lower() in text:
                        categorycount = {}
                        if datestr in datecount:
                            categorycount = datecount[datestr]
                        if key in categorycount:
                            categorycount[key] += 1
                        else:
                            categorycount[key] = 1
                        datecount[datestr] = categorycount
                        break

        datekeys = set(datecount.keys())
        dinfos = []

        # For each datekeys generate a DateInfo class object and append
        for date in datekeys:
            d = datetime.strptime(date, self.dayhoursdm)
            if d:
                info = DateInfo()
                info.d = d
                info.catcounts = datecount[date]
                dinfos.append(info)

        # Sort in descending order of the dates
        dinfos.sort(reverse=True)

        # Assign asixsteps according to number of categories and dates
        result["axisxstep"] = len(dinfos) - 1
        result["axisystep"] = len(self.CATEGORIES) - 1
        xcoordinates = []
        ycoordinates = []
        axisxlabels = []
        axisylabels = []
        data = []
        for key in catkeys:
            axisylabels.append(key)
        i = 0
        j = 0
        for date in dinfos:
            strdate = date.d.strftime(self.hoursdm)
            axisxlabels.append(strdate)
            catcounts = date.catcounts
            for key in catkeys:
                xcoordinates.append(j)
                ycoordinates.append(i)
                i += 1
                if key in catcounts:
                    data.append(catcounts[key])
                else:
                    data.append(0)
            i = 0
            j += 1
        result["xcoordinates"] = xcoordinates
        result["ycoordinates"] = ycoordinates
        result["axisxlabels"] = axisxlabels
        result["axisylabels"] = axisylabels
        result["data"] = data
        return result


def get_data(result):
    """
    Function to generate Event summary from the Categories given
    :return:
    """
    # global infile_name
    ese = EventSummaryExtractor()

    ese.initialize_categories()

    return ese.extract_category_trends(result)


if __name__ == '__main__':
    global infile_name
    ese = EventSummaryExtractor()
    # parser = argparse.ArgumentParser()
    # parser.add_argument('-i', nargs="?", default=ese.DEF_INFILENAME,
    #                     help='Name of the input file containing tweets')
    # print(get_data(result))
    textTotal = []
    for x in result:
        groupInfo = [x['coordinates'], x['_id'], x['username'], x['text'].lower(), x['place_name'], x['place_country'],
                     x['place_coordinates'], x['date']]
        textTotal.append(groupInfo)
    for x in textTotal:
        # print(datetime.strptime(x[7], "%a %b %d %H:%M:%S Z %Y"))
        timeArry = datetime.strptime(x[7], "%a %b %d %H:%M:%S +0000 %Y")
        # timestamp = time.mktime(timeArry)
        x[7] = datetime.timestamp(timeArry)
        # print(timeArry)
    print(get_data(textTotal))
    # print(textTotal)
