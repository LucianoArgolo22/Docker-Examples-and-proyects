from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import random
from datetime import timedelta

args={
'owner' : 'arocketman',
'start_date': days_ago(1)
}
dag = DAG(dag_id='my_sample_dag', default_args=args, schedule_interval=None)

def run_this_func(**context):
    print("hi")

def randomly_failed(**context):
    if random.random() > 0.7:
        raise Exception("It Failed")
    print("It still keeps on working great")

def push_to_xcom(**context):
    random_value = random.random()
    context["ti"].xcom_push(key="random_value", value=random_value)
    print()

def print_xcom(**context):
    pulled_value = context["ti"].xcom_pull(key="random_value")
    print(f'this is the pulled value {pulled_value}')

with dag:
    run_this_task = PythonOperator(
        task_id='run_this',
        python_callable=run_this_func,
        provide_context=True
    )
    run_this_task2 = PythonOperator(
        task_id='run_this_randomly',
        python_callable=randomly_failed,
        provide_context=True,
        retry=4,
        retry_delay=timedelta(seconds=2)
    )
    run_this_task3 = PythonOperator(
    task_id='run_this3',
    python_callable=push_to_xcom,
    provide_context=True,
    retry=4,
    retry_delay=timedelta(seconds=2)
    )
    run_this_task4 = PythonOperator(
    task_id='run_this4',
    python_callable=print_xcom,
    provide_context=True,
    retry=4,
    retry_delay=timedelta(seconds=2)
    )

    run_this_task >> run_this_task2
    run_this_task2 >> run_this_task3
    run_this_task3 >> run_this_task4