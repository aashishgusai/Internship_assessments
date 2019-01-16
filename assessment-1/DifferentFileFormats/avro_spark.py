'''
write a program to implement avro file read/write operation.
Run using -> spark-submit --packages org.apache.spark:spark-avro_2.11:2.4.0
avro_spark.py 
'''

from pyspark import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext(master = 'local[*]')
spark = SparkSession(sc)

df = spark.read.csv('hdfs:///inputs/dept.csv',header=True,inferSchema=True)

df.write.format('com.databricks.spark.avro').save("hdfs:///inputs/avro_test")

avdf = spark.read.format('com.databricks.spark.avro').load('hdfs:///inputs/avro_test')

avdf.show()

