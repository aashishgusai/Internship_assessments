import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext(master='local[*]')
spark=SparkSession(sc)
fraudDFM = spark.read.parquet("hdfs:///inputs/complex_dags/ccFraudM.parquet")

fraudDFF = spark.read.parquet("hdfs:///inputs/complex_dags/ccFraudF.parquet")
#fraudDFM.show()
fraudDFiltM=fraudDFM.filter(fraudDFM.numTrans > 20)
fraudDFiltF=fraudDFF.filter(fraudDFF.numTrans > 20)
fraudDFilter=fraudDFiltM.union(fraudDFiltF)
# fraudDFilter.show()
fraudDFilter.write.parquet("hdfs:///inputs/complex_dags/ccFraudFilter_numTrans.parquet")
