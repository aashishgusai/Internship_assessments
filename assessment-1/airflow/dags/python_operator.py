from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
	'owner':'airflow',
	'email':['airflow@example.com'],
	'depends_upon_past':False,
	'start_date':datetime(2019,1,2),
	'email_on_failure':False,
	'email_on_retry':False,
	'retries':1,
	'retry_delay':timedelta(minutes=1),
}
dag = DAG('python_operator_test',default_args=default_args)

def func_hello():
  print("Hello from a function")

def my_function(fname):
  print(fname + " is cool!")

run_this = PythonOperator(
	task_id='my_function',
	python_callable=my_function,
	op_kwargs={'fname':'Python'},
	dag=dag,
)


task = PythonOperator(
	task_id='func_hello',
	python_callable=func_hello,
	dag=dag
)

run_this >> task

