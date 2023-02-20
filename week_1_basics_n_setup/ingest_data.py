import pandas as pd
import sys
import urllib.request
import os
import sqlalchemy
from time import time
import argparse

parser = argparse.ArgumentParser(description='Ingest CSV data to postgres')

# user
# password
# host 
# port 
# database name
# table name
# url of the csv


def main(params):
    # add parameters
    user        = params.user
    password    = params.password
    host        = params.host
    port        = params.port
    db          = params.db
    table       = params.table
    url_csv     = params.url_csv
    csv_name    = 'output.csv.gz'
    
    # download csv
    urllib.request.urlretrieve(url_csv, filename=os.getcwd() + f'/data/{csv_name}')
    
    #create connetcion to postgres
    engine = sqlalchemy.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # convert dataframe to panda's chunks that can help with upload to database
    temp_iter = pd.read_csv(f'data/{csv_name}', chunksize=100000, iterator=True)

    # see first chunk of itereter table
    df = next(temp_iter)
    
    # connect to database
    engine.connect()
    # return only table's header and create table in database
    df.head(0).to_sql(con=engine, name=table, if_exists='replace')
    # create loop for upload all chunk to table in postgres
    while True:
        t_start = time()
        df = next(temp_iter)
        df = df.astype({
        'tpep_pickup_datetime':'datetime64[ns]',
        'tpep_dropoff_datetime':'datetime64[ns]'
        })
        df.to_sql(con=engine, name=table, if_exists='append')
        t_end = time()
        print('insert another chunk ... , it took %.3f second' %(t_end - t_start)) # it anotation that can allow you put measure
    
    engine.close()

if __name__ == "__main__":
        # create parameters with argparse library
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table', help='name of table for postgres')
    parser.add_argument('--url_csv', help='url of the csv file')

    args = parser.parse_args()
    main(args)





