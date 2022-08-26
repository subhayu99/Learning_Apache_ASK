from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def first_function_execute(*args, **kwargs):
    name = kwargs.get("name", "noName!")
    print(f"Hello {name}")
    return f"Hello {name}"


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

    first_function_execute = PythonOperator(
        task_id="first_function_execute",
        python_callable=first_function_execute,
        op_kwargs={"name": "Subhayu"}
    )