import tinytuya
import pymongo
import asyncio
import threading
from bson import ObjectId
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from meross_iot.http_api import MerossHttpClient
from meross_iot.manager import MerossManager
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

load_dotenv()

URL = os.getenv("URL")
EMAIL = os.getenv("EMAIL")
PASSWORD = os.getenv("PASSWORD")
SENDER = os.getenv("SENDER")
PASS = os.getenv("PASS")
RECEIVER = [os.getenv("REM_EMAIL"), os.getenv("MAYA_EMAIL")]
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
API_DEVICE = [os.getenv("API_DEVICE_10"), os.getenv("API_DEVICE_20"), os.getenv("API_DEVICE_30")]

INTERVAL = timedelta(seconds=60)
DAY = timedelta(hours=24)
HALF_INTERVAL = INTERVAL/2
DEVICES_SIZE = [8, 6, 9]

docs = [{}, {}, {}]
ids = ['64d1548894895e0b4c1bc07f','64d154d494895e0b4c1bc081','64d154bc94895e0b4c1bc080']
names = ['ayat', 'qater', 'ward']
check = [[True, True], [True, True], [True, True]]

dev_map = {
    'fridge_10': '64d160d293d44252699aa218',
    'charger_10': '64d1609b93d44252699aa217',
    'food_processor_10': '64d15f9393d44252699aa215',
    'tv_10': '64d1605493d44252699aa216',
    'fridge_11': '64d161e193d44252699aa219',
    'lamp_10': '64d161fd93d44252699aa21a',
    'office_strip_10': '64d1629393d44252699aa21b',
    'tv_11': '64d162bf93d44252699aa21c',
    'fridge_20': '64d1646b93d44252699aa221',
    'coffee_maker_20': '64d1641a93d44252699aa220',
    'lamp_20': '64d1648093d44252699aa222',
    'charger_20': '64d162ff93d44252699aa21d',
    'fan_20': '64d1638293d44252699aa21e',
    'hair_dryer_20': '64d163dd93d44252699aa21f',
    'cooler_30': '64d1656693d44252699aa225',
    'toaster_30': '64d1685693d44252699aa22b',
    'charger_30': '64d1682993d44252699aa22a',
    'lamp_30': '64d1659d93d44252699aa226',
    'air_fryer_30': '64d1650b93d44252699aa223',
    'camera_30': '64d167a193d44252699aa228',
    'speaker_30': '64d167e593d44252699aa229',
    'tv_30': '64d1687493d44252699aa22c',
    'office_strip_30': '64d165e693d44252699aa227',
}

def send_email(subject, body, receiver):
    try:
        message = MIMEMultipart()
        message["From"] = SENDER
        message["To"] = receiver
        message["Subject"] = subject
        message.attach(MIMEText(body, "plain"))
        smtp_server = "smtp.gmail.com"
        smtp_port = 587 
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()  
            server.login(SENDER, PASS)
            server.sendmail(SENDER, receiver, message.as_string())

    except Exception as e:
        print("Error: Unable to send email.")
        print(e)

async def meross():
    http_api_client = await MerossHttpClient.async_from_user_password(email=EMAIL, password=PASSWORD)
    manager = MerossManager(http_client=http_api_client)
    await manager.async_init()
    await manager.async_device_discovery()
    meross_devices = manager.find_devices(device_type="mss310")
    for dev in meross_devices:
        await dev.async_update()
        
    prev_ts = prev_timestamp = datetime.now()
    mid = True
    while True:
        current_timestamp = datetime.now()
        if current_timestamp - prev_ts >= DAY:
            manager.close()
            await http_api_client.async_logout()
            http_api_client = await MerossHttpClient.async_from_user_password(email=EMAIL, password=PASSWORD)
            manager = MerossManager(http_client=http_api_client)
            await manager.async_init()
            prev_ts += DAY
        
        if current_timestamp - prev_timestamp >= HALF_INTERVAL:
            if mid: 
                for dev in meross_devices:
                    reading = await dev.async_get_instant_metrics()
                    docs[0][dev_map[dev.name]] = reading.power
            mid = not mid 
            prev_timestamp += HALF_INTERVAL              
    
def get_pow():
    cloud = [tinytuya.Cloud(
            apiRegion="eu", 
            apiKey=API_KEY, 
            apiSecret=API_SECRET, 
            apiDeviceID=api) for api in API_DEVICE]
    
    devices = [c.getdevices() for c in cloud]
     
    prev_timestamp = datetime.now()
    mid = True
    while True:
        current_timestamp = datetime.now()
        if current_timestamp - prev_timestamp >= HALF_INTERVAL:
            if mid: 
                for i in range(3):
                    docs[i]['user'] = ObjectId(ids[i])
                    for dev in devices[i]:
                        connected = cloud[i].getconnectstatus(dev['id'])
                        if connected:
                            result = cloud[i].getstatus(dev['id'])
                            state = result['result'][4]['value']
                            docs[i][dev_map[dev['name']]] = state/10.0
                        else:
                            docs[i][dev_map[dev['name']]] = None
            mid = not mid  
            prev_timestamp += HALF_INTERVAL
                                
def validate():
    for i in range(len(names)):
        if len(docs[i].keys()) == DEVICES_SIZE[i] + 3:
            check[i][0] = True
            j = 0
            for key, value in docs[i].items():
                if key == 'timestamp' or key == 'user' or key == '_id':
                    continue
                if not isinstance(value, (int, float)):
                    if check[i][1]:
                        check[i][1] = False
                        for email in RECEIVER:
                            send_email('ERROR', f'Null values for user {names[i]}', email)
                    break
                j += 1
            if j == DEVICES_SIZE[i]: check[i][1] = True  
        elif check[i][0]:
            check[i][0] = False
            for email in RECEIVER:
                send_email('ERROR', f'One or more of the devices is missing for user {names[i]}', email)  
                 
def insert_into_db():
    client = pymongo.MongoClient(URL)
    db = client['hemsproject']
    collection = db['PowerReadings']
    prev_timestamp = datetime.now()
    while True:
        current_timestamp = datetime.now()
        if current_timestamp - prev_timestamp >= INTERVAL:
            prev_timestamp += INTERVAL
            for doc in docs:
                doc['timestamp'] = prev_timestamp
                
            collection.insert_many(docs)   
            # validate()
            print(f'{docs[0]["timestamp"]}: done')
            for doc in docs:
                doc.clear()
            

pow_collector = threading.Thread(target=get_pow)
db_inserter = threading.Thread(target=insert_into_db)

pow_collector.start()
db_inserter.start()

# asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())    
loop = asyncio.get_event_loop()
loop.run_until_complete(meross())
loop.stop()
    

    
    

