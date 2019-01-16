import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext(master='local[2]')
spark=SparkSession(sc)

df = spark.read.json('hdfs:///inputs/example_1.json')

df.printSchema()

df.show()

df.createOrReplaceTempView("example1")

capital_name = spark.sql("SELECT fruit FROM example1")
capital_name.show()


