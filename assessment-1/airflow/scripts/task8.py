import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame
from functools import reduce

def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

sc = SparkContext(master='local[*]')
spark=SparkSession(sc)
fraudDF1 = spark.read.parquet("hdfs:///inputs/complex_dags/ccFraudFilter_numTrans1.parquet")

fraudDF2 = spark.read.parquet("hdfs:///inputs/complex_dags/ccFraudFilter_balance.parquet")

fraudDF3 = spark.read.parquet("hdfs:///inputs/complex_dags/ccFraudFilter_creditLine1.parquet")

fraudDFunion=unionAll(fraudDF1,fraudDF2,fraudDF3)
fraudDFdistinct=fraudDFunion.select('*').dropDuplicates()

fraudDFdistinct.write.format('jdbc').options(url='jdbc:sqlserver://localhost:1433;databaseName=TestDB;user=SA;password=Asg@1234;',dbtable='cc_fraud').save()

fraudShow=spark.read.format('jdbc').options(url='jdbc:sqlserver://localhost:1433;databaseName=TestDB;user=SA;password=Asg@1234;',dbtable='cc_fraud').load()

fraudShow.show()
