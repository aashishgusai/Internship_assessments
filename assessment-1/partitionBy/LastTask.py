from pyspark.sql.session import SparkSession
from pyspark import SparkContext
import pyspark
sc = SparkContext(master = 'local[*]')
spark = SparkSession(sc)

#read data from hdfs 
empDF = spark.read.csv("hdfs:///inputs/Empl*", header= True, inferSchema= True)

#write it to table from hdfs 
empDF.write.format('jdbc').options(url='jdbc:sqlserver://localhost:1433;databaseName=TestDB;user=SA;password=*****;', dbtable='EMP_Table_hadoop').save()

empDFF=spark.read.format('jdbc').options(url='jdbc:sqlserver://localhost:1433;databaseName=TestDB;user=SA;password=*****;',dbtable='EMP_Table_hadoop').load()

#do partitiona-by (categorical-partition) on table 
empDFF.write.partitionBy('DEPARTMENT').parquet('hdfs:///inputs/department')

#and make parquet file of it and read it
empDF1=spark.read.parquet('hdfs:///inputs/department/DEPARTMENT=AVIATION','hdfs:///inputs/department/DEPARTMENT=BUILDINGS')
empDF1.show()

#empDF1=spark.read.parquet('hdfs:///inputs/department/').select('*')
#empDF1.select('*').groupby('DEPARTMENT').count().show()
