import json
from datetime import datetime, timedelta
from textwrap import dedent
import requests
import hashlib
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.python import PythonOperator 
from airflow.operators.bash_operator import BashOperator
from textblob import TextBlob
from google.cloud import bigquery
import pandas as pd
import numpy as np
import airflow
import os
import time
import pytz

project_id = "is3107-384006" 
dataset_id = "sentiment_analysis" 
avg_polarity_score_table_id = "avg_polarity_score"
comments_table_id = "comments"

default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : airflow.utils.dates.days_ago(0),
    'retries' : 1,
    'retry_delay' : timedelta(seconds = 60)
}

dag = DAG(
    'reddit_hdb_bto_sentiment_analysis', 
    default_args = default_args,
    description = 'Perform sentiment analysis on comments on posts in r/singapore about HDB or BTO',
    schedule_interval = timedelta(days = 1)
)

def get_oauth_token(execution_date, **context) :
    headers = {'User-Agent' : 'PostmanRuntime/7.31.3'}
    token_data = context['ti'].xcom_pull(key = 'token_data', task_ids = 'get_oauth_token')
    if token_data is not None and 'token_data' in token_data:
        token_expiration = datetime.strptime(token_data['expiration'], '%Y-%m-%dT%H:%M:%S.%f').replace(tzinfo = pytz.UTC)
        if execution_date < token_expiration:
            return token_data

    URL = 'https://www.reddit.com/api/v1/access_token?grant_type=client_credentials&scope=read'
    username = 'HugmDi2SeXjzrQfA8ZZudg'
    password = 'vcJN7GKAlK5ypvbe-9Crzfa3L7gduw'

    response = requests.post(URL, auth = (username, password), headers = headers)
    time.sleep(1)
    token = response.json()
    expires_in = token.get('expires_in')
    if expires_in is None:
        return {'access_token' : '', 'expiration' : ''}
    token_data = {'access_token' : token.get("access_token"),
                  'expiration' : (datetime.utcnow() + timedelta(seconds = expires_in)).isoformat()}
    context['ti'].xcom_push(key = 'token_data', value = token_data)
    return token_data

def get_post_id(**context) :
    token = context['task_instance'].xcom_pull(task_ids = 'get_oauth_token')
    headers = {'User-Agent' : 'Mozilla/5.0', 'Authorization' : f'Bearer {token["access_token"]}'}

    URL = 'https://oauth.reddit.com/r/singapore/search?restrict_sr=1&sort=new&limit=100&q=hdb%20bto'
    response = requests.get(URL, headers = headers)
    print(response)
    response_data = response.json()['data']
    post_ids = [post["data"]["id"] for post in response_data['children']]
    return json.dumps(post_ids)

def get_comments(post_id, **context) :
    token = context['task_instance'].xcom_pull(task_ids = 'get_oauth_token')
    post_ids = json.loads(post_id)
    output = []
    headers = {'User-Agent' : 'Mozilla/5.0', 'Authorization' : f'Bearer {token["access_token"]}'}

    for post_id in post_ids :
        print(post_id)
        URL = 'https://oauth.reddit.com/comments/' + f'{post_id}'
        response = requests.get(URL, headers = headers)
        response_data = response.json()[1]['data']
        for comment in response_data['children'] :
            if 'body' in comment['data'] :
                print(comment['data']['body'])
                blob = TextBlob(comment['data']['body'])
                polarity_score = blob.sentiment.polarity
                output += [{'body' : comment['data']['body'], 'created_utc' : comment['data']['created_utc'], 'polarity_score': polarity_score}]
    return json.dumps(output)

def perform_sentiment_analysis(comments, **context) :
    sentiment_scores = []
    print(type(comments))
    comments_list = json.loads(comments)
    print(type(comments_list))
    for comment in comments_list :
        blob = TextBlob(comment['body'])
        sentiment_scores.append(blob.sentiment.polarity)
    return json.dumps(sentiment_scores)

def write_sentiment_scores(sentiment_scores, **context) :
    sentiment_scores = json.loads(sentiment_scores)
    print(sentiment_scores)
    return np.mean(sentiment_scores)

def create_dataset_if_not_exist(project_id, dataset_id):
    bq_client = bigquery.Client(project = project_id)
    dataset_ref = bq_client.dataset(dataset_id)
    try:
        bq_client.get_dataset(dataset_ref)
    except :
        dataset = bigquery.Dataset(dataset_ref)
        dataset = bq_client.create_dataset(dataset)

def insert_avg_polarity_score_to_bigquery(avg_polarity_score, **context):
    bq_client = bigquery.Client(project = project_id)
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(avg_polarity_score_table_id)

    try :
        bq_client.get_table(table_ref)
    except :
        schema = [
            bigquery.SchemaField('avg_polarity_score', 'FLOAT', mode = 'NULLABLE'),
            bigquery.SchemaField('date', 'TIMESTAMP', mode = 'NULLABLE')
        ]
        table = bigquery.Table(table_ref, schema = schema)
        bq_client.create_table(table)
        print("Table {} created.".format(avg_polarity_score_table_id))

    row_to_insert = {
        'avg_polarity_score' : avg_polarity_score, 
        'date' : datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    errors = bq_client.insert_rows_json(table_ref, [row_to_insert])
    if errors:
        print("Encountered errors while inserting rows: {}".format(errors))
    else:
        print("Rows inserted successfully.")

def insert_comments_to_bigquery(comments, **context):
    comments = json.loads(comments)
    print(datetime.now())
    
    bq_client = bigquery.Client(project = project_id)
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id = comments_table_id)
    
    try:
        bq_client.delete_table(table_ref)
        print(f"Deleted table {comments_table_id}")
    except :
        print(f"Table {comments_table_id} not found. Creating new table...")
    
    schema = [
        bigquery.SchemaField("comment_id", "STRING", mode = "NULLABLE"),
        bigquery.SchemaField("comment_text", "STRING", mode = "NULLABLE"),
        bigquery.SchemaField("date", "TIMESTAMP", mode = "NULLABLE"), 
        bigquery.SchemaField("polarity_score", "FLOAT", mode = "NULLABLE")
    ]

    table = bigquery.Table(table_ref, schema=schema)
    table = bq_client.create_table(table)
    print(f"Created table {table.table_id}")
    
    rows_to_insert = []
    for comment in comments:
        comment_id = hashlib.md5((comment['body'] + str(comment['created_utc'])).encode('utf-8')).hexdigest()
        row_to_insert = {
            'comment_id' : comment_id,
            'comment_text' : comment['body'], 
            'date' : datetime.utcfromtimestamp(comment['created_utc']).strftime('%Y-%m-%d %H:%M:%S'), 
            'polarity_score' : comment['polarity_score']
        }
        rows_to_insert.append(row_to_insert)

    errors = bq_client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        print("Encountered errors while inserting rows: {}".format(errors))
    else:
        print("Rows inserted successfully.")

create_dataset_if_not_exist(project_id, dataset_id)

get_oauth_token_task = PythonOperator(
    task_id = 'get_oauth_token',
    python_callable = get_oauth_token,
    provide_context = True,
    dag = dag
)

get_post_id_task = PythonOperator(
    task_id = 'get_post_id',
    python_callable = get_post_id,
    provide_context = True,
    dag = dag
)

get_comments_task = PythonOperator(
    task_id = 'get_comments',
    python_callable = get_comments,
    provide_context = True,
    op_kwargs = {'post_id' : '{{ task_instance.xcom_pull(task_ids="get_post_id") }}'},
    dag = dag
)

perform_sentiment_analysis_task = PythonOperator(
    task_id = 'perform_sentiment_analysis',
    python_callable = perform_sentiment_analysis,
    provide_context = True,
    op_kwargs = {'comments' : '{{ task_instance.xcom_pull(task_ids="get_comments") }}'},
    dag = dag
)

write_sentiment_scores_task = PythonOperator(
    task_id = 'write_sentiment_scores',
    python_callable = write_sentiment_scores,
    provide_context = True,
    op_kwargs = {'sentiment_scores' : '{{ task_instance.xcom_pull(task_ids="perform_sentiment_analysis") }}'},
    dag = dag
)

insert_avg_polarity_score_to_bigquery_task = PythonOperator(
    task_id = 'insert_avg_polarity_score_to_bigquery',
    python_callable = insert_avg_polarity_score_to_bigquery,
    provide_context = True,
    op_kwargs = {'avg_polarity_score' : '{{ task_instance.xcom_pull(task_ids="write_sentiment_scores") }}'},
    dag = dag
)

insert_comments_to_bigquery_task = PythonOperator(
    task_id = 'insert_comments_to_bigquery',
    python_callable = insert_comments_to_bigquery,
    provide_context = True,
    op_kwargs = {'comments' : '{{ task_instance.xcom_pull(task_ids="get_comments") }}'},
    dag = dag
)

get_oauth_token_task >> get_post_id_task >> get_comments_task >> perform_sentiment_analysis_task >> write_sentiment_scores_task >> insert_avg_polarity_score_to_bigquery_task
get_oauth_token_task >> get_post_id_task >> get_comments_task >> perform_sentiment_analysis_task >> write_sentiment_scores_task
get_oauth_token_task >> get_post_id_task >> get_comments_task >> insert_comments_to_bigquery_task


