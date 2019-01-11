from pyspark.sql.session import SparkSession
from pyspark import SparkContext
import pyspark
import yaml
import yaycl_crypt
import yaycl

sc = SparkContext(master = 'local[*]')
spark = SparkSession(sc)

conf = yaycl.Config('/Internship_assessments/DonorsChooseCode/', crypt_key='/Internship_assessments/DonorsChooseCode/secret_text')
#yaycl_crypt.encrypt_yaml(conf, 'config')
yaycl_crypt.decrypt_yaml(conf, 'config')
file1=open("/Internship_assessments/DonorsChooseCode/config.yaml")
yaycl_crypt.encrypt_yaml(conf, 'config')

cfg=yaml.load(file1)

def colRename_wrtprq(DF ,prq_name):
	for col in DF.columns:
		DF = DF.withColumnRenamed(col,col.replace(" ", "_"))
	try:
		DF.write.parquet("hdfs:///inputs/DonorChoose/"+prq_name+".parquet")
	except:
		yield	

#read data from hdfs 
try:
	donationsDF = spark.read.parquet("hdfs:///inputs/DonorChoose/donations.parquet")
except:
	donationsDF = spark.read.csv("hdfs:///inputs/DonorChoose/Donations.csv", header= True, inferSchema= True)
	colRename_wrtprq(donationsDF ,'donations')
	
try:
	donorsDF = spark.read.parquet("hdfs:///inputs/DonorChoose/donors.parquet")
except:
	donorsDF = spark.read.csv("hdfs:///inputs/DonorChoose/Donors.csv", header= True, inferSchema= True)
	colRename_wrtprq(donorsDF ,'donors')

try:
	projectsDF = spark.read.parquet("hdfs:///inputs/DonorChoose/projects.parquet")
except:	
	projectsDF = spark.read.csv("hdfs:///inputs/DonorChoose/Projects.csv", header= True, inferSchema= True)
	colRename_wrtprq(projectsDF ,'projects')

try:
	resourcesDF = spark.read.parquet("hdfs:///inputs/DonorChoose/resources.parquet")
except:
	resourcesDF = spark.read.csv("hdfs:///inputs/DonorChoose/Resources.csv", header= True, inferSchema= True)
	colRename_wrtprq(resourcesDF ,'resources')

try:
	schoolsDF = spark.read.parquet("hdfs:///inputs/DonorChoose/schools.parquet")
except:
	schoolsDF = spark.read.csv("hdfs:///inputs/DonorChoose/Schools.csv", header= True, inferSchema= True)
	colRename_wrtprq(schoolsDF ,'schools')

try:
	TeachersDF = spark.read.parquet("hdfs:///inputs/DonorChoose/teachers.parquet")
except:
	TeachersDF = spark.read.csv("hdfs:///inputs/DonorChoose/Teachers.csv", header= True, inferSchema= True)
	colRename_wrtprq(TeachersDF ,'teachers')
