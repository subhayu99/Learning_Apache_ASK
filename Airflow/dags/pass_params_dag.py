from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def first_function(**context):
    print("First function")
    context['ti'].xcom_push(key='mykey', value='first function says Hello')


def second_function(**context):
    instance = context.get('ti').xcom_pull(key='mykey')
    print("Second function")
    print(f"Value passed: {instance}")
    return f"Second {instance}"


with DAG(
    dag_id="first_dag",
    schedule_interval="@daily",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2022, 6, 25)
    },
    catchup=False) as f:

    first_function = PythonOperator(
        task_id="first_function",
        python_callable=first_function,
        provide_context=True,
        op_kwargs={"name": "Subhayu"}
    )

    second_function = PythonOperator(
        task_id="second_function",
        python_callable=second_function,
        provide_context=True,
        op_kwargs={"name": "Subhayu"}
    )