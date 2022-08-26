from pymongo import MongoClient
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from pymongo.cursor import Cursor


def fetch_kitchen_order():
    hook = MongoHook('atlas_cluster')
    data: Cursor = hook.find(mongo_collection='kitchenOrder', mongo_db='qopla', query={})
    for i in data:
        print(i)


def using_pymongo():
    client = MongoClient('mongodb+srv://subhayu:subhayu@cluster0.cr1ss.mongodb.net/')
    db = client['qopla']
    collection = db['kitchenOrder']

    data = collection.find_one({"orderNo": 1})
    print(data)


with DAG(
    dag_id="fetch_data_from_mongodb",
    schedule_interval="@hourly",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2022, 6, 25)
    },
    catchup=False,
    tags=['mongodb']) as f:

    fetch_kitchen_order = PythonOperator(
        task_id="fetch_kitchen_order",
        python_callable=fetch_kitchen_order
    )

    using_pymongo = PythonOperator(
        task_id="using_pymongo",
        python_callable=using_pymongo
    )