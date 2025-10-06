
import pandas as pd
from pandas.io.sql import get_schema
from sqlalchemy import create_engine
import os

def ingest_callable(user, password, host, port, db, table_name, parquet_file ):
    print(table_name, parquet_file)
  

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()


    df = pd.read_parquet(parquet_file)


    print(get_schema(df,table_name, con=engine))

    df.to_sql(
    name=table_name,
    con=engine,
    if_exists='replace',
    index=False,
    method='multi',
    chunksize=10000
)

