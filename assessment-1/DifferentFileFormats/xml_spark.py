import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext(master='local[2]')
spark=SparkSession(sc)
#required external package for XML operation
df = spark.read.format('com.databricks.spark.xml').options(rowTag='T').load('hdfs:///inputs/partsupp.xml')

# df.show()

df.write.format('jdbc').options(url='jdbc:sqlserver://localhost:1433;databaseName=TestDB;user=SA;password=*****;',dbtable='xml_table').save()

dftable=spark.read.format('jdbc').options(url='jdbc:sqlserver://localhost:1433;databaseName=TestDB;user=SA;password=*****;', dbtable='xml_table').load()

dftable.show()
