
import pymongo
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
URL = os.getenv("URL")

client = pymongo.MongoClient(URL)
db = client['hemsproject']
collection = db['PowerReadings']
cutoff_date = pd.Timestamp('2023-08-20 00:00:00')

filter_query = {"timestamp": {"$lt": cutoff_date}}
delete_result = collection.delete_many(filter_query)

client.close()















