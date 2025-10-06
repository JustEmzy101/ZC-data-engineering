
import pandas as pd
from pandas.io.sql import get_schema
from sqlalchemy import create_engine
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.parquet'
    os.system(f"wget {url} -O {csv_name}")
    # User
    # PW
    # host
    # port
    # database name
    # table name
    # url of the parquet file

    

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')


    engine.connect()


    df = pd.read_parquet(csv_name)


    print(get_schema(df,table_name, con=engine))

    df.to_sql(
    name=table_name,
    con=engine,
    if_exists='replace',
    index=False,
    method='multi',
    chunksize=100000
)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Parquet data to postgres')
    parser.add_argument('--user',help='username for postgres')
    parser.add_argument('--password',help='password for postgres')
    parser.add_argument('--host',help='host for postgres')
    parser.add_argument('--port',help='port for postgres')
    parser.add_argument('--db',help='database name for postgres')
    parser.add_argument('--table_name',help='table name where results will be in')
    parser.add_argument('--url',help='url of the parquet file')

    args = parser.parse_args()
    #print(args.accumulate(args.integers))

    main(args)