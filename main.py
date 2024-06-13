import requests
import pandas as pd

import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector.pandas_tools import write_pandas

from datetime import datetime
import os

SNOWFLAKE_USER = os.environ['SNOWFLAKE_USER']
SNOWFLAKE_PASS = os.environ['SNOWFLAKE_PASS']
SNOWFLAKE_ACCOUNT = os.environ['SNOWFLAKE_ACCOUNT']
SNOWFLAKE_WAREHOUSE = os.environ['SNOWFLAKE_WAREHOUSE']


def fetch_chain_tvl():
    url = 'https://api.llama.fi/v2/historicalChainTvl/arbitrum'
    response = requests.get(url)
    data = response.json()
    arbitrum_tvl = pd.DataFrame(data)
    arbitrum_tvl['date'] = arbitrum_tvl['date'].apply(lambda x: datetime.fromtimestamp(x))
    arbitrum_tvl['date'] = arbitrum_tvl['date'].apply(lambda x: datetime.strftime(x, '%Y-%m-%d %H:%M:%S'))
    return arbitrum_tvl

def execute_sql(sql_string, **kwargs):
  conn = snowflake.connector.connect(user=SNOWFLAKE_USER,
                                     password=SNOWFLAKE_PASS,
                                     account=SNOWFLAKE_ACCOUNT,
                                     warehouse=SNOWFLAKE_WAREHOUSE,
                                     database="SCROLLSTATS",
                                     schema="DBT_SCROLLSTATS")

  sql = sql_string.format()
  res = conn.cursor(DictCursor).execute(sql)
  results = res.fetchall()
  conn.close()
  return results


def increment_table(df, **kwargs):
    df.columns = df.columns.str.upper() 
    
    conn = snowflake.connector.connect(user=SNOWFLAKE_USER,
                                     password=SNOWFLAKE_PASS,
                                     account=SNOWFLAKE_ACCOUNT,
                                     warehouse=SNOWFLAKE_WAREHOUSE,
                                     database="ARBIGRANTS",
                                     schema="DBT")
    
    cursor = conn.cursor()
    
    table_name = "ARBIGRANTS_ONE_TOTAL_TVL"
    create_table_query_tvl = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        DATE TIMESTAMP_NTZ,
        TVL NUMBER(18,0)
    );
    """
    cursor.execute(create_table_query_tvl)

    for _, row in df.iterrows():
        date = row['DATE']
        tvl = row['TVL']

        merge_query_tvl = f"""
        MERGE INTO {table_name} AS target
        USING (
            SELECT
                TO_DATE(%s) AS DATE,
                %s AS TVL
        ) AS source
        ON target.DATE = source.DATE
        WHEN NOT MATCHED THEN
            INSERT (DATE, TVL)
            VALUES (source.DATE, source.TVL);
        """
        cursor.execute(merge_query_tvl, (date, tvl))

    conn.commit()
    cursor.close()
    conn.close()

    print("DataFrame successfully written to the tables.")

arbitrum_tvl = fetch_chain_tvl()
increment_table(arbitrum_tvl)