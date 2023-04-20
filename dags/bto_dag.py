from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import bigquery
import requests
import airflow

project_id = "is3107-384006"
dataset_id = "bto"
table_id = "price_ranges"
# gcs_bucket = "{{var.value.gcs_bucket}}"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bto_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=30),
)

"""
Queries the BTO price ranges API based on limit, offset and sorting 
"""
def get_latest_transactions(limit=50, offset=0, ascending=True):
    url = "https://data.gov.sg/api/action/datastore_search?"
    sort = "_id asc" if ascending else "_id desc"
    params = {"resource_id":"d23b9636-5812-4b33-951e-b209de710dd5", 
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
    processed_records = []

    for record in records:
        record['max_selling_price'] = float(record['max_selling_price'])
        record['min_selling_price'] = float(record['min_selling_price'])
        record['min_selling_price_less_ahg_shg'] = float(record['min_selling_price_less_ahg_shg'])
        record['max_selling_price_less_ahg_shg'] = float(record['max_selling_price_less_ahg_shg'])
        record['financial_year'] = int(record['financial_year'])
        record['town'] = record['town'].upper()
        num_room_in_string = record['room_type'].split('-')[0]
        record['room_type'] = num_room_in_string + " ROOM"
        del record["_id"]
        
        if record['max_selling_price'] == 0 and record['min_selling_price'] == 0 and record['min_selling_price_less_ahg_shg'] == 0 and record['max_selling_price_less_ahg_shg'] == 0:
            continue
        processed_records += [record]
            
    return processed_records


"""
Retrieves all price data from latest year and passes year variable
"""
def get_transactions_latest_year():
    latest_transactions = []
    offset, limit = 0, 1000
    while True:
        records = get_latest_transactions(ascending=False, limit=limit, offset=offset)
        records = process_transactions(records)
        latest_year = records[0]['financial_year']
        records = list(filter(lambda x: x['financial_year'] >= latest_year, records))
        latest_transactions.extend(records)
        offset += limit
        if len(records) < limit:
            break
    return latest_transactions

def get_latest_year():
    records = get_latest_transactions(ascending=False, limit=1)
    records = process_transactions(records)
    latest_year = records[0]['financial_year']
    return latest_year

def delete_rows():
    year = get_latest_year()
    sql_query = f"DELETE FROM {project_id}.{dataset_id}.{table_id} WHERE financial_year >= {year}"
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
    table_ref = bq_client.dataset(dataset_id=dataset_id).table(table_id=table_id)
    errors = bq_client.insert_rows_json(table_ref, transactions)
    if errors:
        print("Encountered errors while inserting rows: {}".format(errors))
    else:
        print("Rows inserted successfully.")

with dag:

    delete_rows_task = delete_rows()

    insert_data_task = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data,
    )
    
    delete_rows_task >> insert_data_task 