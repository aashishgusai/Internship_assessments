from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
import os
import sys

os.environ['SPARK_HOME']='/usr/local/spark'
sys.path.append(os.path.join(os.environ['SPARK_HOME'],'bin'))
os.environ['HADOOP_CONF_DIR']='/usr/local/hadoop/etc/hadoop'


default_args = {
    	'owner':'airflow',
	'email':['airflow@example.com'],
	'depends_upon_past':False,
	'email_on_failure':False,
	'email_on_retry':False,
	'retries':1,
	'retry_delay':timedelta(minutes=1),
}

dag = DAG('complex_dags_bash', default_args=default_args, start_date=(datetime(2018,12,17)))

t1 = BashOperator(	
	task_id='task1',
	bash_command='spark-submit --master yarn  /Internship_assessments/assessment-1/airflow/scripts/task1.py
 && echo "Task-1 is Done" ',
	dag=dag)
t2 = BashOperator(	
	task_id='task2',
	bash_command='spark-submit --master yarn /Internship_assessments/assessment-1/airflow/scripts/task2.py && echo "Task-2 is Done" ',
	dag=dag)
t3 = BashOperator(	
	task_id='task3',
	bash_command='spark-submit --master yarn /Internship_assessments/assessment-1/airflow/scripts/task3.py && echo "Task-3 is Done" ',
	trigger_rule='all_done',
	dag=dag)
t4 = BashOperator(	
	task_id='task4',
	bash_command='spark-submit --master yarn /Internship_assessments/assessment-1/airflow/scripts/task4.py && echo "Task-4 is Done" ',
	trigger_rule='all_done',
	dag=dag)
t5 = BashOperator(	
	task_id='task5',
	bash_command='spark-submit --master yarn /Internship_assessments/assessment-1/airflow/scripts/task5.py && echo "Task-5 is Done" ',
	trigger_rule='all_done',
	dag=dag)
t6 = BashOperator(	
	task_id='task6',
	bash_command='spark-submit --master yarn /Internship_assessments/assessment-1/airflow/scripts/task6.py && echo "Task-6 is Done" ',
	trigger_rule='all_done',
	dag=dag)
t7 = BashOperator(	
	task_id='task7',
	bash_command='spark-submit --master yarn /Internship_assessments/assessment-1/airflow/scripts/task7.py && echo "Task-7 is Done" ',
	dag=dag)
t8 = BashOperator(	
	task_id='task8',
	bash_command='spark-submit --master yarn /Internship_assessments/assessment-1/airflow/scripts/task8.py && echo "Task-8 is Done" ',
	trigger_rule='all_done',
	dag=dag)
  
  #setup dependencies 
t1 >> t3 >> t4 >> t8 #t3 depence on t1
t2 >> t3 
t5 << t3 >> t7 >> t5 >> t6  >> t8 << t7
