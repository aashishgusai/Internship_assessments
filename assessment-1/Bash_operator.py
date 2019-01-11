from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

default_args = {
    	'owner':'airflow',
	'email':['airflow@example.com'],
	'depends_upon_past':False,
	'email_on_failure':False,
	'email_on_retry':False,
	'retries':1,
	'retry_delay':timedelta(minutes=1),
}
dag = DAG('bash_operator_test', default_args=default_args, start_date=(datetime(2019,1,2)))

t1 = BashOperator(	
	task_id='test1',
	bash_command='sleep 10 && echo "T1 is Done" ',
	trigger_rule='all_done',
	dag=dag)

t2 = BashOperator(	
	task_id='test2',
	bash_command='sleep 20 && echo "T2 is Done"',
	trigger_rule='all_done',
	dag=dag)

t3 = BashOperator(	
	task_id='test3',
	bash_command='sleep 10 && echo "T1 is Done"',
	trigger_rule='all_done',
	dag=dag)


[t1, t2] >> t3
# t2 << t3

