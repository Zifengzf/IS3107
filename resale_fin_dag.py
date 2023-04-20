from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
import requests
import airflow
import itertools
import numpy as np
import pandas as pd
import math
import datetime as dt
from datetime import datetime
import yfinance as yf
from pandas_datareader.data import DataReader

import requests_cache
session = requests_cache.CachedSession(cache_name='cache', backend='sqlite')
session.headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
                   'Accept': 'application/json;charset=utf-8'}

data_dict = {'10Y_Treasury': '^TNX', 'S&P': '^GSPC', 'STI': '^STI'}


project_id = "graphical-reach-380414" 
dataset_id = "hdb" 
resale_table_id = "resale_transactions" 
financial_table_id = "financial_data"
combine_table_id = "combine_data"
input_data_id = "input_data"
output_data_id = "output_data"
model_id = "linear"


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'transactions_updating',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

''' Model Input Data Creation '''

def get_financial_data():
    query = f"SELECT _10Y_Treasury, S_P, STI FROM {project_id}.{dataset_id}.{financial_table_id} ORDER BY Date DESC LIMIT 1"
    client = bigquery.Client()
    query_job = client.query(query) 
    results = query_job.result()
    for row in results:
        return dict(row)

def get_unique_col(col_name='town'):
    query = f"SELECT DISTINCT {col_name} FROM {project_id}.{dataset_id}.{resale_table_id}"
    client = bigquery.Client()
    query_job = client.query(query) 
    results = query_job.result()
    return [row[col_name] for row in results]

def predict_prices(initial, periods, col='S_P', case='bull'):
    growth = {'S_P': {'bull': 1.1, 'base': 1, 'bear': 0.9}, 'STI': {'bull': 1.05, 'base': 1, 'bear': 0.95}}
    growth_rate = growth[col][case] if col in growth else 1
    return initial * growth_rate ** periods

def upload_table(df):
    client = bigquery.Client()
    table_id = f'{dataset_id}.{input_data_id}'
    job_config = bigquery.LoadJobConfig(
        # specify the write disposition to overwrite the table
        write_disposition='WRITE_TRUNCATE',
        # specify the create disposition to create the table if it does not exist
        create_disposition='CREATE_IF_NEEDED',
        # specify additional options, such as the delimiter and null marker
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter=',',
        null_marker='',
        )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

def generate_input_data_upload():
    financials = get_financial_data()
    base_year = get_latest_year()
    years = [base_year + i for i in range(1, 6)]
    cases = ['bull', 'bear', 'base']
    features = ['town', 'flat_type', 'storey_range']
    values = [get_unique_col(feature) for feature in features] + [years] + [cases]
    combinations = list(itertools.product(values[0], values[1], values[2], values[3], values[4]))
    df = pd.DataFrame(combinations, columns=features+['year', 'case'])
    for stat in financials:
        df[stat] = df.apply(lambda row: predict_prices(financials[stat], row['year']-base_year, stat, row['case'] ), axis=1)
    upload_table(df)


''' Model Creation and Prediction '''

def update_model(model_type='LINEAR_REG', label='resale_price'):
    query = f'''
        CREATE OR REPLACE MODEL {dataset_id}.{model_id} 
        OPTIONS(model_type='{model_type}', input_label_cols=['{label}']) AS
        SELECT
            _10Y_Treasury,
            S_P,
            STI,
            town,
            flat_type,
            storey_range,
            resale_price,
            year
        FROM `{project_id}.{dataset_id}.{combine_table_id}`
        WHERE {label} IS NOT NULL
    '''
    
    task = BigQueryInsertJobOperator(
        task_id="update_model",
        configuration={
            "query": {
                "query": query,
                "useLegacySql": False,
                }
            },
        dag=dag
    )
    return task

def model_predict():
    query = f'''
    CREATE OR REPLACE TABLE {dataset_id}.{output_data_id} AS
    SELECT * FROM
        ML.PREDICT(MODEL {dataset_id}.{model_id}, (
        SELECT * FROM `{project_id}.{dataset_id}.{input_data_id}`))
    '''
    task = BigQueryInsertJobOperator(
        task_id="model_predict",
        configuration={
            "query": {
                "query": query,
                "useLegacySql": False,
                }
            },
        dag=dag
    )
    return task


''' Yahoo Finance '''

"""
Queries the Yahoo Finance API for latest data
"""
def get_latest_yf():
    
    date_today = datetime.today() - dt.timedelta(days=11)
    date_ysd = (date_today - dt.timedelta(days=1))
    latest_record = (date_ysd - dt.timedelta(days=1))

    data_present = True
    new_dict = {'Date': date_ysd.strftime('%Y-%m')}

    # Attempt to get yesterday's data
    for key, value in data_dict.items():
        yf_df = yf.download(value, date_ysd-dt.timedelta(days=1), date_ysd)
        if len(yf_df) == 0:
            data_present = False
        else:
            new_dict[key] = yf_df.iloc[0]['Adj Close']

    if data_present:

        # Check if it is the last day of the month
        if date_ysd.month == latest_record.month:
            # If same month, delete old data then add
            delete_rows_from_yf(date_ysd.strftime('%Y-%m'))
        
        insert_rows_yf([new_dict])


def delete_rows_from_yf(idx_mth_yr):
    sql_query = f"DELETE FROM {project_id}.{dataset_id}.{financial_table_id} WHERE Date = '{idx_mth_yr}'"
    task = BigQueryInsertJobOperator(
        task_id="delete_rows_yf",
        configuration={
            "query": {
                "query": sql_query,
                "useLegacySql": False,
                }
            },
        dag=dag
    )
    return task

def insert_rows_yf(df):
    bq_client = bigquery.Client(project=project_id)
    table_ref = bq_client.dataset(dataset_id=dataset_id).table(table_id=financial_table_id)
    errors = bq_client.insert_rows_json(table_ref, df)
    if errors:
        print("Encountered errors while inserting rows: {}".format(errors))
    else:
        print("Rows inserted successfully.")


''' Resale Housing Data '''

"""
Queries the HDB resale transactions API based on limit, offset and sorting 
"""
def get_latest_transactions(limit=50, offset=0, ascending=True):
    url = "https://data.gov.sg/api/action/datastore_search?"
    sort = "_id asc" if ascending else "_id desc"
    params = {"resource_id":"f1765b54-a209-4718-8d38-a39237f502b3", 
              "limit":limit, 
              "offset": offset, 
              "sort": sort}
    try: 
        resp = requests.get(url=url, params=params).json()
        result = resp['result']
        records = result['records']
        return records

    except Exception as e:
        print(e)

"""
Process fields by converting data types and deleting _id field
"""
def process_transactions(records):
    for record in records:
        record['resale_price'] = float(record['resale_price'])
        record['floor_area_sqm'] = float(record['floor_area_sqm'])
        record['lease_commence_date'] = int(record['lease_commence_date'])
        record['remaining_lease'] = int(record['remaining_lease'].split()[0])
        record['year'] = int(record['month'].split('-')[0])
        record['month'] = int(record['month'].split('-')[1])
        del record['_id']

    return records


"""
Retrieves all transactions from latest year and passes year variable
"""
def get_transactions_latest_year():
    latest_transactions = []
    offset, limit = 0, 1000
    while True:
        records = get_latest_transactions(ascending=False, limit=limit, offset=offset)
        records = process_transactions(records)
        latest_year = records[0]['year']
        records = list(filter(lambda x: x['year'] >= latest_year, records))
        latest_transactions.extend(records)
        offset += limit
        if len(records) < limit:
            break
    return latest_transactions

def get_latest_year():
    records = get_latest_transactions(ascending=False, limit=1)
    records = process_transactions(records)
    latest_year = records[0]['year']
    return latest_year

def delete_rows():
    year = get_latest_year()
    print(f"Year: {year}")
    # sql_query = f"DELETE FROM {project_id}.{dataset_id}.{resale_table_id} WHERE year >= '{year}'"
    sql_query = f"DELETE FROM {project_id}.{dataset_id}.{resale_table_id} WHERE year >= {year}"
    task = BigQueryInsertJobOperator(
        task_id="delete_rows",
        configuration={
            "query": {
                "query": sql_query,
                "useLegacySql": False,
                }
            },
        dag=dag
    )
    return task

def insert_data():
    transactions = get_transactions_latest_year()
    bq_client = bigquery.Client(project=project_id)
    table_ref = bq_client.dataset(dataset_id=dataset_id).table(table_id=resale_table_id)
    errors = bq_client.insert_rows_json(table_ref, transactions)
    if errors:
        print("Encountered errors while inserting rows: {}".format(errors))
    else:
        print("Rows inserted successfully.")


''' Combine Data '''


def del_combine_data():
    year = get_latest_year()
    sql_query = f"DELETE FROM {project_id}.{dataset_id}.{combine_table_id} WHERE year >= {year}"
    task = BigQueryInsertJobOperator(
        task_id="delete_combine",
        configuration={
            "query": {
                "query": sql_query,
                "useLegacySql": False,
                }
            },
        dag=dag
    )
    return task

def insert_combine_data():

    year = get_latest_year()

    combined_table = f"{project_id}.{dataset_id}.{combine_table_id}"
    table1 = f"{project_id}.{dataset_id}.{financial_table_id}"
    table2 = f"{project_id}.{dataset_id}.{resale_table_id}"
    columns = ','.join(["a.month","town","flat_type","block","street_name","storey_range","floor_area_sqm",
               "flat_model","lease_commence_date","resale_price","remaining_lease","a.year","Date",
               "_10Y_Treasury","S_P","STI"])
    
    sql_query = f"INSERT INTO {combined_table} SELECT {columns} FROM {table1} a JOIN {table2} b ON a.year=b.year AND a.month=b.month WHERE b.year={year}"


    task = BigQueryInsertJobOperator(
        task_id="insert_combine",
        configuration={
            "query": {
                "query": [sql_query],
                "useLegacySql": False,
                }
            },
        dag=dag
    )
    return task

with dag:

    yf_update_data_task = PythonOperator(
        task_id='yf_update_data',
        python_callable=get_latest_yf,
    )

    resale_delete_rows_task = delete_rows()

    resale_insert_data_task = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data,
    )

    upload_input_data_task = PythonOperator(
        task_id='upload_input_data',
        python_callable=generate_input_data_upload
    )

    combine_delete_task = del_combine_data()

    combine_insert_task = insert_combine_data()

    update_model_task = update_model()

    model_predict_task = model_predict()

    yf_update_data_task >> combine_delete_task
    yf_update_data_task >> upload_input_data_task
    resale_delete_rows_task >> resale_insert_data_task
    resale_insert_data_task >> combine_delete_task
    combine_delete_task >> combine_insert_task
    combine_insert_task >> update_model_task
    update_model_task >> model_predict_task