import datetime
from pymongo import MongoClient


client = MongoClient('mongodb://localhost:27017/')
db = client['qopla']
collection = db['kitchenOrder']

SHOPID = '5c3c4c07febd2d0001c433f4'
sDay,sMonth,sYear = 18,4,2020
eDay,eMonth,eYear = 30,4,2020
start = datetime.datetime(sYear, sMonth, sDay, 0, 0, 0, 0)
end = datetime.datetime(eYear, eMonth, eDay, 23, 59, 59, 381)

data = collection.find({'createdAt': {'$gte': start, '$lt': end}, "shopId": SHOPID})
print(list(data))