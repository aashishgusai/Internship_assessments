import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext(master='local[*]')
spark=SparkSession(sc)
fraudDF = spark.read.csv("hdfs:///inputs/ccFraud.csv",header=True,inferSchema=True)
fraudDFM=fraudDF.filter(fraudDF.gender == 1)
# fraudDFM.show()
fraudDFM.write.parquet("hdfs:///inputs/complex_dags/ccFraudM.parquet")
