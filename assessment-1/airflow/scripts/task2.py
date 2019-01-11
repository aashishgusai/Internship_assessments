import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext(master='local[*]')
spark=SparkSession(sc)
fraudDF = spark.read.csv("hdfs:///inputs/ccFraud.csv",header=True,inferSchema=True)
fraudDFF=fraudDF.filter(fraudDF.gender == 2)
# fraudDFF.show()
fraudDFF.write.parquet("hdfs:///inputs/complex_dags/ccFraudF.parquet")
