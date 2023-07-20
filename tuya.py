import tinytuya
import pymongo
import asyncio
import threading
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
RECEIVER = os.getenv("RECEIVER")
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
API_DEVICE = os.getenv("API_DEVICE")

INTERVAL = timedelta(seconds=30)
HALF_INTERVAL = INTERVAL/2
tuya_devices = []
readings = {}
doc = {}
DEVICES_SIZE = 6

def init():
    global cloud
    cloud = tinytuya.Cloud(
            apiRegion="eu", 
            apiKey=API_KEY, 
            apiSecret=API_SECRET, 
            apiDeviceID=API_DEVICE)

    for dev in cloud.getdevices():
        if dev['product_name'] != 'Smart Plug':
            continue
        tuya_devices.append(dev)
        readings[dev['id']] = []
    
    client = pymongo.MongoClient(URL)
    db = client['paws&stripes']
    collection = db['hems']
    return collection

def send_email(subject, body):
    try:
        message = MIMEMultipart()
        message["From"] = SENDER
        message["To"] = RECEIVER
        message["Subject"] = subject
        message.attach(MIMEText(body, "plain"))
        smtp_server = "smtp.gmail.com"
        smtp_port = 587 
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()  
            server.login(SENDER, PASS)
            server.sendmail(SENDER, RECEIVER, message.as_string())

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
            if len(doc.keys()) == DEVICES_SIZE + 2:
                for key, value in doc.items():
                    if key == 'timestamp':
                        continue
                    if not isinstance(value, (int, float)):
                        send_email('ERROR', 'Null values')
                        break
            else:
               send_email('ERROR', 'One or more of the devices is missing') 
    
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
    

    
    

