"""
- Nama  : Firman
- Batch : BSD - 007
- Objective : This app is used to get data from postgresql, clean the data, and insert to elasticsearch
"""


import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch

def queryPostgresql(): #function to get data from postgresql and save to csv
    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn=db.connect(conn_string)
    df=pd.read_sql("select * from table_m3",conn)
    df.to_csv('/opt/airflow/dags/P2M3_firman_data_raw.csv')
    print("-------Data Saved------")

# function to drop missing values, drop duplicat, and change lowercase table name and save to csv
def cleanData():
    df=pd.read_csv('/opt/airflow/dags/P2M3_firman_data_raw.csv') # read csv
    df.dropna(inplace=True)     # drop missing values                   
    df.drop_duplicates(inplace=True) # drop duplicate
    df.columns=[x.lower() for x in df.columns] # change column name to lowercase
    df.columns = df.columns.str.replace(' ', '_') # change column name with space to underscore
    df['age'] = df['age'].astype('int64') # change data type to int64
    df['clothing_id'] = df['clothing_id'].astype('int64') # change data type to int64
    df['rating'] = df['rating'].astype('int64') # change data type to int64
    df['recommended_ind'] = df['recommended_ind'].astype('int64') # change data type to int64
    df['positive_feedback_count'] = df['positive_feedback_count'].astype('int64') # change data type to int64
    df.rename(columns={'unnamed:_0': 'id'}, inplace=True) # change column name

    df.to_csv('/opt/airflow/dags/P2M3_firman_data_cleaned.csv') # save to csv after cleaning
    print("-------Data Cleaned------")

# function to insert data to elasticsearch
def insertElasticsearch():
    es = Elasticsearch('http://elasticsearch:9200') # connect to elasticsearch
    df=pd.read_csv('/opt/airflow/dags/P2M3_firman_data_cleaned.csv') # read cleaned data
    es.indices.delete(index="firman", ignore=[400, 404]) # delete index firman if exist
    for i,r in df.iterrows(): # insert data to elasticsearch
        doc=r.to_json() # convert to json
        res=es.index(index="firman",doc_type="doc",body=doc)    # insert to elasticsearch
        print(res)  # print result
    print("-------Data Inserted to Elasticsearch------")

default_args = {
    'owner': 'firman',
    'start_date': dt.datetime(2024, 9, 12), # start date
    'retries': 1, # number of retries
    'retry_delay': dt.timedelta(minutes=0.1) # retry delay
}


with DAG('MyDBdag',
         default_args=default_args,
         schedule_interval='30 6 * * *',      # every 6.30 daily (UTC time)
         ) as dag:

    getData = PythonOperator(task_id='QueryPostgreSQL',
                                 python_callable=queryPostgresql) # call function to get data from postgresql

    cleanData = PythonOperator(task_id='CleanData',
                                    python_callable=cleanData)  # call function to clean data
    
    insertData = PythonOperator(task_id='InsertDataElasticsearch',
                                 python_callable=insertElasticsearch) # call function to insert data to elasticsearch



getData >> cleanData >> insertData # set task dependency
