import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext(master='local[*]')
spark=SparkSession(sc)
fraudDF = spark.read.parquet("hdfs:///inputs/complex_dags/ccFraudFilter_numTrans.parquet")

fraudDFilter=fraudDF.filter((fraudDF.creditLine > 20) & (fraudDF.creditLine < 30))
#fraudDFilter.show()
fraudDFilter.write.parquet("hdfs:///inputs/complex_dags/ccFraudFilter_creditLine2.parquet")
