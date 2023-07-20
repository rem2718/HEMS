import tinytuya
import pymongo
import os
import asyncio
import threading
from datetime import datetime, timedelta
from meross_iot.http_api import MerossHttpClient
from meross_iot.manager import MerossManager

URL = "mongodb+srv://rem:paws2020@paws-and-stripes.ovdm3cw.mongodb.net/paws&stripes?retryWrites=true&w=majority"
INTERVAL = timedelta(seconds=30)
HALF_INTERVAL = INTERVAL/2
EMAIL =  "mayakhalide2001@gmail.com"
PASSWORD = "smarthomemaya"
tuya_devices = []
readings = {}
doc = {}

def init():
    global cloud
    cloud = tinytuya.Cloud(
            apiRegion="eu", 
            apiKey="cqtnjcjfqx7dyp8rxnk9", 
            apiSecret="8d368c35107241d988addde26e99d37b", 
            apiDeviceID="bf7445bf05ce9c706dyuxd")

    for dev in cloud.getdevices():
        if dev['product_name'] != 'Smart Plug':
            continue
        tuya_devices.append(dev)
        readings[dev['id']] = []
    
    client = pymongo.MongoClient(URL)
    db = client['paws&stripes']
    collection = db['hems']
    return collection


async def meross():
    http_api_client = await MerossHttpClient.async_from_user_password(email=EMAIL, password=PASSWORD)
    manager = MerossManager(http_client=http_api_client)
    await manager.async_init()
    await manager.async_device_discovery()
    meross_devices = manager.find_devices(device_type="mss310")
    for dev in meross_devices:
        await dev.async_update()
    prev_timestamp = datetime.now()
    mid = True
    while True:
        current_timestamp = datetime.now()
        if current_timestamp - prev_timestamp >= HALF_INTERVAL:
            if mid: 
                for dev in meross_devices:
                    reading = await dev.async_get_instant_metrics()
                    doc[dev.name] = reading.power
            prev_timestamp += HALF_INTERVAL
            mid != mid 
    
    manager.close()
    await http_api_client.async_logout()
        
    
def get_pow():
    prev_timestamp = datetime.now()
    mid = True
    while True:
        current_timestamp = datetime.now()
        if current_timestamp - prev_timestamp >= HALF_INTERVAL:
            if mid: 
                for dev in tuya_devices:
                    result = cloud.getstatus(dev['id'])
                    state = result['result'][4]['value']
                    doc[dev['name']] = state/10.0
            prev_timestamp += HALF_INTERVAL
            mid != mid 
  
def insert_document(document, collection):
    if "_id" in document:
        del document["_id"] 
    collection.insert_one(document)              
         
def insert_into_db(collection):
    prev_timestamp = datetime.now()
    while True:
        current_timestamp = datetime.now()
        if current_timestamp - prev_timestamp >= INTERVAL:
            prev_timestamp += INTERVAL
            doc['timestamp'] = prev_timestamp
            insert_document(doc, collection)
            print(f'{doc["timestamp"]}: done')



collection = init()    

pow_collector = threading.Thread(target=get_pow)
db_inserter = threading.Thread(target=insert_into_db, args=(collection,))

pow_collector.start()
db_inserter.start()


asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())    
loop = asyncio.get_event_loop()
loop.run_until_complete(meross())
loop.stop()
    

    
    

