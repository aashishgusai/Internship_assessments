from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators import SparkSubmitOperator
import os
import sys


os.environ['SPARK_HOME']='/usr/local/spark'
sys.path.append(os.path.join(os.environ['SPARK_HOME'],'bin'))
os.environ['HADOOP_CONF_DIR']='/usr/local/hadoop/etc/hadoop'

APPLICATION_FILE_LOC='/home/hduser/airflow/scripts/'

default_args = {
    	'owner':'airflow',
	'email':['airflow@example.com'],
	'depends_upon_past':False,
	'email_on_failure':False,
	'email_on_retry':False,
	'retries':1,
	'retry_delay':timedelta(minutes=1),
}

dag = DAG('complex_dags_sso', default_args=default_args, start_date=(datetime(2019,1,4)))

t1 = SparkSubmitOperator(	
	task_id='task1',
	application_file=APPLICATION_FILE_LOC+'task1.py',
	master='yarn',
	dag=dag)
t2 = SparkSubmitOperator(	
	task_id='task2',
	application_file=APPLICATION_FILE_LOC+'task2.py',
	master='yarn',
	dag=dag)
t3 = SparkSubmitOperator(	
	task_id='task3',
	application_file=APPLICATION_FILE_LOC+'task3.py',
	master='yarn',
	trigger_rule='all_done',
	dag=dag)
t4 = SparkSubmitOperator(	
	task_id='task4',
	application_file=APPLICATION_FILE_LOC+'task4.py',
	master='yarn',
	trigger_rule='all_done',
	dag=dag)
t5 = SparkSubmitOperator(	
	task_id='task5',
	application_file=APPLICATION_FILE_LOC+'task5.py',
	master='yarn',
	trigger_rule='all_done',
	dag=dag)
t6 = SparkSubmitOperator(	
	task_id='task6',
	application_file=APPLICATION_FILE_LOC+'task6.py',
	master='yarn',
	trigger_rule='all_done',
	dag=dag)
t7 = SparkSubmitOperator(	
	task_id='task7',
	application_file=APPLICATION_FILE_LOC+'task7.py',
	master='yarn',
	dag=dag)
t8 = SparkSubmitOperator(	
	task_id='task8',
	application_file=APPLICATION_FILE_LOC+'task8.py',
	master='yarn',
	trigger_rule='all_done',
	dag=dag)

t1 >> t3 >> t4 >> t8 #t3 depence on t1
t2 >> t3 # >> t5

t5 << t3 >> t7 >> t5 >> t6  >> t8 << t7
#t7 >> t8


