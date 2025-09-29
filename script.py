import requests
import os
import csv
from dotenv import load_dotenv
load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit=1000&sort=ticker&apiKey={POLYGON_API_KEY}'
response = requests.get(url)
tickers = []
data = response.json()
for ticker in data['results']:
    tickers.append(ticker) 
while 'next_url' in data:
    next_url = data['next_url'] + f'&apiKey={POLYGON_API_KEY}'
    response = requests.get(next_url)
    data = response.json()
    if ('results'in data):
        for ticker in data['results']:  
            tickers.append(ticker)
print(len(tickers))

example_ticker = {'ticker': 'BAMB', 
'name': 'Brookstone Intermediate Bond ETF', 
'market': 'stocks', 
'locale': 'us', 
'primary_exchange': 'BATS', 
'type': 'ETF', 
'active': True, 
'currency_name': 'usd', 
'composite_figi': 'BBG01JG4ZQ50', 
'share_class_figi': 'BBG01JG4ZR03', 
'last_updated_utc': '2025-09-28T06:04:49.245538295Z'}

fieldnames = list(example_ticker.keys())
print(fieldnames)
output_csv = 'tickers.csv'
with open(output_csv,mode = 'w',newline ='',encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames = fieldnames)
    writer.writeheader()
    for t in tickers:
        row ={key : t.get(key,'') for key in fieldnames} 
        writer.writerow(row)
    print(f'{len(tickers)} to {output_csv}')


