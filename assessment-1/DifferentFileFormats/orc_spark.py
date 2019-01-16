from pyspark import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext(master = 'local[*]')
spark = SparkSession(sc)

df = spark.read.csv('hdfs:///inputs/dept.csv',header=True,inferSchema=True)

df.write.orc("hdfs:///inputs/orc_test")
orcdf = spark.read.orc('hdfs:///inputs/orc_test')

orcdf.show()
