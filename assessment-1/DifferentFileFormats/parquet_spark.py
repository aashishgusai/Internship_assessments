'''
write a program to implement Parquet file read/write operation with mssql Database.
'''
import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import db_conf as db

sc = SparkContext(master='local[*]')
spark=SparkSession(sc)
cfg=db.cfg

empDFF=spark.read.format(cfg['type'].get('conn_type')).options(url=cfg['url'].get('path'),dbtable='emp').load()

#empDFF.write.parquet("hdfs:///inputs/EMP_Table_hadoop.parquet")

#empDF = spark.read.parquet("hdfs:///inputs/EMP_Table_hadoop.parquet")
#empDF.show()

