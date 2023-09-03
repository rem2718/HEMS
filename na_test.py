import pandas as pd

ayat = pd.read_csv('ayat.csv', parse_dates=['timestamp'])
qater = pd.read_csv('qater.csv', parse_dates=['timestamp'])
ward = pd.read_csv('ward.csv', parse_dates=['timestamp'])

ayat.drop(['user', '_id'], inplace=True, axis=1)
qater.drop(['user', '_id'], inplace=True, axis=1)
ward.drop(['user', '_id'], inplace=True, axis=1)

ts =  pd.Timestamp('2023-08-26 01:52:00')



dev_map = {
    'fridge_10': '64d160d293d44252699aa218',
    'charger_10': '64d1609b93d44252699aa217',
    'food_processor_10': '64d15f9393d44252699aa215',
    'tv_10': '64d1605493d44252699aa216',
    
    'fridge_11': '64d161e193d44252699aa219',
    'lamp_10': '64d161fd93d44252699aa21a',
    'office_strip_10': '64d1629393d44252699aa21b',
    'tv_11': '64d162bf93d44252699aa21c', 
 
}

inv_map = {v: k for k, v in dev_map.items()}

ayat = ayat[ayat['timestamp'] >= ts]
ayat['energy'] = (ayat['64d162bf93d44252699aa21c'] * 1/60) / 1000

e = ayat['energy'].sum()

# qater = qater[qater['timestamp'] > ts]
# ward = ward[ward['timestamp'] > ts]

# ayat_na = ayat.isnull().sum()
# qater_na = qater.isnull().sum()
# ward_na = ward.isnull().sum()

# ayat_na = ayat_na[ayat_na != 0].rename(index=inv_map) 
# qater_na = qater_na[qater_na != 0].rename(index=inv_map) 
# ward_na = ward_na[ward_na != 0].rename(index=inv_map) 

# print(f'ayat:\n{ayat_na}')
# print(f'qater:\n{qater_na}')
# print(f'ward:\n{ward_na}')
