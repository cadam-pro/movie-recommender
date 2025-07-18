from airflow import DAG
from datetime import datetime
from airflow.decorators import task

def hello():
    print("Hello from Airflow!")

with DAG(
    dag_id="hello_world",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    
    @task()
    def hello_word():
        print("Hello !")
hello_word = hello_word()

hello_word