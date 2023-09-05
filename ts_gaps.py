
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

ayat = ObjectId('64d1548894895e0b4c1bc07f')
qater = ObjectId('64d154d494895e0b4c1bc081')
ward = ObjectId('64d154bc94895e0b4c1bc080')

df = pd.DataFrame(list(collection.find({'user': ayat})))

df['timestamp'] = pd.to_datetime(df['timestamp'])
df.sort_values(by='timestamp', inplace=True)
df.reset_index(inplace=True)
cutoff_date = pd.Timestamp('2023-08-20 00:00:00')
df = df[df['timestamp'] >= cutoff_date]

def change_timestamp(timestamp, new_second, new_millisecond):
    return timestamp.replace(second=new_second, microsecond=new_millisecond * 1000)

df['timestamp'] = df['timestamp'].apply(lambda x: change_timestamp(x, 0, 0))


# filling long gaps manually by subsituting old data 
ts =  pd.Timestamp('2023-08-17 19:34:00')
ts2 =  pd.Timestamp('2023-08-17 22:05:00')
df = df[(df['timestamp'] > ts) & (df['timestamp'] < ts2)]
null_counts = df.isnull().sum()

start_time =  pd.Timestamp('2023-08-17 19:35:00')
end_time =  pd.Timestamp('2023-08-17 22:04:00')
missing_timestamps = pd.date_range(start_time, end_time, freq='1T')

for timestamp in missing_timestamps:
    row = df[df['timestamp'] == timestamp]
    row['timestamp'] = timestamp.replace(day=20)
    del row['index']
    del row['_id']
    doc = row.to_dict(orient='records')[0]
    collection.insert_one(doc)
    


# filling the timestamp gaps backward for +30mins gaps
time_diff = df['timestamp'].diff()
gaps = df[time_diff > pd.Timedelta('30 minutes')]['timestamp']
gaps = pd.DataFrame(gaps)
gaps['start'] = None
gaps = gaps.rename(columns={'timestamp': 'end'})
for idx in gaps.index:
    gaps.at[idx, 'start'] = df.at[idx-1, 'timestamp']

gaps['diff'] = gaps['end'] - gaps['start']

start_time = df['timestamp'].min()
end_time = df['timestamp'].max()
missing_timestamps = pd.date_range(start_time, end_time, freq='1T').difference(df['timestamp'])

long_gaps = pd.DatetimeIndex([])
for idx in gaps.index:
    start_time = gaps.at[idx, 'start']
    end_time = gaps.at[idx, 'end']
    long_gaps = long_gaps.union(pd.date_range(start_time, end_time, freq='1T'))

missing_timestamps = missing_timestamps.difference(long_gaps)

for timestamp in missing_timestamps:
    row = df[df['timestamp'] >  timestamp].head(1)
    row['timestamp'] = timestamp
    del row['index']
    del row['_id']
    doc = row.to_dict(orient='records')[0]
    collection.insert_one(doc)
    
    
    



















