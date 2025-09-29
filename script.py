import requests
import os
import csv
import snowflake.connector
from dotenv import load_dotenv
from datetime import datetime
load_dotenv()

def run_stock_job():
    DS = datetime.now().strftime('%y-%m-%d')
    POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
    url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit=1000&sort=ticker&apiKey={POLYGON_API_KEY}'    
    response = requests.get(url)
    tickers = []
    data = response.json()
    for ticker in data['results']:
        ticker['ds'] = DS
        tickers.append(ticker) 
    while 'next_url' in data:
        next_url = data['next_url'] + f'&apiKey={POLYGON_API_KEY}'
        response = requests.get(next_url)
        data = response.json()
        if ('results'in data):
            for ticker in data['results']:  
                ticker['ds'] = DS
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
    'last_updated_utc': '2025-09-28T06:04:49.245538295Z',
    'ds': '2025-9-29'}

    fieldnames = list(example_ticker.keys())
    output_csv = 'tickers.csv'
    with open(output_csv,mode = 'w',newline ='',encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames = fieldnames)
        writer.writeheader()
        for t in tickers:
            row ={key : t.get(key,'') for key in fieldnames} 
            writer.writerow(row)
    print(f'{len(tickers)} to {output_csv}')

def load_data_to_snowflake():

        #grab the connection parameters from env
        #connect to your warehouse using snowflake connector
        try:
            conn = snowflake.connector.connect(
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                role=os.getenv("SNOWFLAKE_ROLE")
                )
            print('connection successful')
        except Exception as e:
            print("connection refused:", e)
            return
        curr = conn.cursor()
        #extract the data schema from the data warehouse
        try:
            curr.execute (f'DESC TABLE {os.getenv("SNOWFLAKE_SCHEMA")}.{os.getenv("SNOWFLAKE_TABLE")}')
            schema = {row[0] : row[1] for row in curr.fetchall()}
            print(schema)
            #build the query for the insert
            columns = ', '.join(schema.keys())
            placeholders = ', '.join(['%s']* len(schema))
            print(columns)
            print(placeholders)
            query =f'insert into {os.getenv("SNOWFLAKE_TABLE")} ({columns}), VALUES ({placeholders})'
            print(query)
        except Exception as e:
            print("Cannot connect to the table:", e)
if __name__ == '__main__':
   load_data_to_snowflake()
   #run_stock_job()



