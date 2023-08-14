import pandas as pd

ayat = pd.read_csv('ayat.csv', parse_dates=['timestamp'])
qater = pd.read_csv('qater.csv', parse_dates=['timestamp'])
ward = pd.read_csv('ward.csv', parse_dates=['timestamp'])

ts =  pd.Timestamp('2023-08-14 1:00:00')

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

inv_map = {v: k for k, v in dev_map.items()}

ayat = ayat[ayat['timestamp'] > ts]
qater = qater[qater['timestamp'] > ts]
ward = ward[ward['timestamp'] > ts]

ayat_na = ayat.isnull().sum()
qater_na = qater.isnull().sum()
ward_na = ward.isnull().sum()

ayat_na = ayat_na[ayat_na != 0].rename(index=inv_map) 
qater_na = qater_na[qater_na != 0].rename(index=inv_map) 
ward_na = ward_na[ward_na != 0].rename(index=inv_map) 

print(f'ayat:\n{ayat_na}')
print(f'qater:\n{qater_na}')
print(f'ward:\n{ward_na}')
