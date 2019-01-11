import Load_Data as op

TeachersDF=op.TeachersDF
donationsDF=op.donationsDF
donorsDF=op.donorsDF
projectsDF=op.projectsDF
resourcesDF=op.resourcesDF
schoolsDF=op.schoolsDF

sc = op.sc
spark = op.spark

donor_donation = donationsDF.join(donorsDF,['DonorID'])

project_resource=projectsDF.join(resourcesDF,['ProjectID'],'left_outer')

school_project=projectsDF.join(schoolsDF,['SchoolID'],'right_outer')

school_project.select('ProjectTitle','SchoolName').show()

#filter
donor_donation.filter(donor_donation.DonationAmount > 50000).count()

#groupby and aggregation
donor_donation.groupby('DonorCity').agg({'DonationAmount':'sum'}).show()

donor_donation.createOrReplaceTempView('donor_donation')

donor_teacher=spark.sql('select * from donor_donation where DonorIsTeacher =="Yes" ')

donor_teacher.write.format(op.cfg['type'].get('conn_type')).options(url=op.cfg['url'].get('path'),dbtable='tbl_donor_teacher').save()

#register for sql operations
TeachersDF.registerTempTable('teacher')

#join 3 table
project_desc=donationsDF.join(projectsDF,['projectID']).join(donorsDF,['DonorID'])

#project which has > 10000 donation
pd_filter=project_desc.filter(project_desc.DonationAmount > 10000)

pd_filter.write.format(op.cfg['type'].get('conn_type')).options(url=op.cfg['url'].get('path'), dbtable='tbl_pd_filter').save()

#sum of amount given in project
project_desc.select('projectType','DonationAmount').groupby('projectType').agg({'DonationAmount':'sum'}).show()

#citywise total a/m
project_desc.select('DonorCity','donationAmount').groupby('DonorCity').agg({'donationAmount':'sum'}).show()

#city with > 10000 donation
data=spark.sql("select DonorCity, sum(DonationAmount) as DA  from project_desc group By DonorCity having DA > 10000 order By DA DESC ")
data.show()

#school with amount of donation
school_data = spark.sql('select SchoolName, sum(DonationAmount) as DA  from school_donation group By SchoolName order By DA DESC')

#map
project_school_donation.rdd.map(lambda x: (x['ProjectType'],x['DonationAmount'])).toDF(['Project','Donation']).show()

#map 
resourcesDF.rdd.map(lambda x: (x['ResourceItemName'],float(x['ResourceQuantity']) * float(x['ResourceUnitPrice']))).toDF(['Resource','Amount']).show()

#Yield 
def my_gen():
	for project_desc in project_desc.rdd.collect():
        	yield print(project_desc)
a =my_gen()
next(a)

#other Examples
def my_gen():
	for project_desc in project_desc.rdd.toLocalIterator():
        	yield print(project_desc)

for item in my_gen():
    print(item) 

#drop null
donor_donation.dropna().count()

#replace nulls
donor_donation.fillna('anyvalue').count()
#Add new Column
donor_donation.withColumn('col_new', donor_donation.donationAmount /2.0).select('donationAmount','col_new').show(5)

#drop column(s)
donor_donation.drop('col_nm').columns

#Count Number of rows
donor_donation.count(),donorsDF.count(),donationsDF.count()

#to see columns
donor_donation.columns
#total columns
len(donor_donation.columns)

#statestical operation numerical columns
donor_donation.describe().show()

donor_donation.describe('DonationAmount').show()

#select mentioned column(s)
donor_donation.select('DonorCity','DonorState').show(5)

#count Distinct
donor_donation.select('DonorCity').distinct().count()

#drop Duplicates
donor_donation.select('DonorCity','DonorState').dropDuplicates().show(10)
