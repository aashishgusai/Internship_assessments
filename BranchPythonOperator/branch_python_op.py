import random

import airflow
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

args ={
	'owner':'airflow',
	'start_date':datetime(2019,1,2),
}

dag = DAG(dag_id='BranchPython_example',default_args=args)

run_this_first = DummyOperator(
	task_id='run_this_first',
	dag=dag
)

options = ['branch_a','branch_b','branch_c','branch_d']

def func_condition():
	return lambda: random.choice(options)


def func_condition1(options):
	if("branch_c" in options):
		return options[2]

branching = BranchPythonOperator(
	task_id='branching',
	python_callable=func_condition1,
	op_kwargs={'options':options},	
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


