import Load_Data as op

spark=op.spark

donor_teacher=spark.read.format(op.cfg['type'].get('conn_type')).options(url=op.cfg['url'].get('path'),dbtable='tbl_donor_teacher1').load()

donor_teacher.columns

pd_filter=spark.read.format(op.cfg['type'].get('conn_type')).options(url=op.cfg['url'].get('path'),dbtable='tbl_pd_filter').load()

#do partitiona-by on table, 

pd_filter.write.partitionBy('DonorState','DonorCity').parquet('hdfs:///inputs/DonorChoose/partition_DS_DC')

#now we have data categories in donorState and donorCity. so we can perform operations on it.

pd_filter=spark.read.parquet('hdfs:///inputs/DonorChoose/partition_DS_DC/').select('*')

pd_filter=spark.read.parquet('hdfs:///inputs/DonorChoose/partition_DS_DC/DonorState=Arizona/DonorCity=Scottsdale/').select('*')

pd_filter=spark.read.parquet('hdfs:///inputs/DonorChoose/partition_DS_DC/DonorState=Arizona/*','hdfs:///inputs/DonorChoose/partition_DS_DC/DonorState=Florida/*')

#pd_filter.show()

#write data as table
pd_filter.write.format('jdbc').options(url='jdbc:postgresql:airflow', dbtable='categorical_data').save() 

pd_filter=spark.read.format('jdbc').options(url='jdbc:postgresql:airflow',dbtable='categorical_data').load()

pd_filter.show()

