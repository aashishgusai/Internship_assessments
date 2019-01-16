import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import yaml

sc = SparkContext(master='local[*]')
spark = SparkSession(sc)

deptDF = spark.read.parquet('hdfs:///inputs/dept.parquet')

#Read file
file1=open("file_path/config.yml")

#Load into yaml configuration
cfg=yaml.load(file1)

#access using yaml object eg. cfg[''].get('')
df_sql = spark.read.format(cfg['type'].get('conn_type')).options(url=cfg['url'].get('path'), dbtable=cfg['dbtable'].get('name')).load()
df_sql.show()
