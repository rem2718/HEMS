
import pymongo
import pandas as pd
import os
from dotenv import load_dotenv
from bson import ObjectId

load_dotenv()
URL = os.getenv("URL")

client = pymongo.MongoClient(URL)
db = client['hemsproject']
collection = db['PowerReadings']

ayat = pd.DataFrame(list(collection.find({'user': ObjectId('64d1548894895e0b4c1bc07f')})))
qater = pd.DataFrame(list(collection.find({'user': ObjectId('64d154d494895e0b4c1bc081')})))
ward = pd.DataFrame(list(collection.find({'user': ObjectId('64d154bc94895e0b4c1bc080')})))

ayat.to_csv('ayat.csv', index=False)
qater.to_csv('qater.csv', index=False)
ward.to_csv('ward.csv', index=False)

