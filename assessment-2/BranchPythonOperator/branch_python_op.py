#based on time one of listed task will run 
#for more see the image attached with content
import airflow
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

args ={
	'owner':'airflow',
	'start_date':datetime(2019,1,14),
}
#fetch current hour
curr_time=int(datetime.now().strftime('%H'))

dag = DAG(dag_id='BranchPython_example',default_args=args)

run_this_first = DummyOperator(
	task_id='run_this_first',
	dag=dag
)

options = ['branch_a-10_15','branch_b-16_20','branch_c-21__','branch_d-last']

def func_condition():	#compair hours
	if(curr_time > 10 and curr_time <= 15): 		
		return options[0]	
	elif(curr_time > 15 and curr_time <= 20):
		return options[1]
	elif(curr_time > 20):
		return options[2]
	else:
		return options[3]


branching = BranchPythonOperator(
	task_id='branching',
	python_callable=func_condition,
	dag=dag
)

run_this_first >> branching

join = DummyOperator(
	task_id='Join',
	trigger_rule='one_success',
	dag=dag
)

for option in options:
	t =DummyOperator(
		task_id=option,
		dag=dag	
	)

	dummy_follow=DummyOperator(
		task_id='follow_' + option,
		dag=dag	
	)

	branching >> t >> dummy_follow >> join

