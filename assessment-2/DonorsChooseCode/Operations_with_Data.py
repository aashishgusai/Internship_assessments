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

project_desc=donationsDF.join(projectsDF,['projectID']).join(donorsDF,['DonorID'])

#States with maximum Donors
donorsDF.select('DonorState').groupby('DonorState').count().orderBy('count',ascending=False).show(10)
'''California, being the state having highest population has also a large number of registered donors base with nearly 300,000 donors. San Francisco and Los Angles are the main cities of California where most number of donors are located. Highest number of schools and projects are also highest from the California state. '''
'''
+--------------+------+                                                         
|    DonorState| count|
+--------------+------+
|    California|294695|
|      New York|137957|
|         Texas|134449|
|       Florida|108828|
|         other|107809|
|      Illinois|104381|
|North Carolina| 84250|
|  Pennsylvania| 72280|
|       Georgia| 63731|
| Massachusetts| 60730|
+--------------+------+
only showing top 10 rows
'''

#States with minimum Donors
donorsDF.select('DonorState').groupby('DonorState').count().orderBy('count').show(10)
'''
+-------------+-----+                                                           
|   DonorState|count|
+-------------+-----+
|      Wyoming| 1634|
| North Dakota| 3012|
|      Vermont| 3677|
| South Dakota| 4084|
|       Alaska| 4519|
|      Montana| 5088|
|     Delaware| 5906|
|     Nebraska| 6272|
|West Virginia| 7497|
|   New Mexico| 7576|
+-------------+-----+
only showing top 10 rows'''

#Number of Teachers and NonTeacher Donors from each state
donorsDF.groupby(['DonorState','DonorIsTeacher']).agg({'DonorIsTeacher':'count'}).orderBy(['DonorState','count(DonorIsTeacher)'],ascending=True).show()
'''
+--------------------+--------------+---------------------+                     
|          DonorState|DonorIsTeacher|count(DonorIsTeacher)|
+--------------------+--------------+---------------------+
|             Alabama|           Yes|                 2188|
|             Alabama|            No|                21124|
|              Alaska|           Yes|                  513|
|              Alaska|            No|                 4006|
|             Arizona|           Yes|                 3839|
|             Arizona|            No|                37690|
|            Arkansas|           Yes|                 1484|
|            Arkansas|            No|                11383|
|          California|           Yes|                25329|
|          California|            No|               269366|
|            Colorado|           Yes|                 2572|
|            Colorado|            No|                30132|
|         Connecticut|           Yes|                 2831|
|         Connecticut|            No|                28773|
|            Delaware|           Yes|                  527|
|            Delaware|            No|                 5379|
|District of Columbia|           Yes|                  922|
|District of Columbia|            No|                 9940|
|             Florida|           Yes|                10547|
|             Florida|            No|                98281|
+--------------------+--------------+---------------------+
only showing top 20 rows
'''

#Projects and their Donations
	#-Projects having Highest number of donors
project_desc.groupby('ProjectID').agg({'ProjectID':'count','DonationAmount':'sum'}).orderBy('count(ProjectID)',ascending=False).show() 
'''
+--------------------+-------------------+----------------+                     
|           ProjectID|sum(DonationAmount)|count(ProjectID)|
+--------------------+-------------------+----------------+
|c34218abf3fecd36b...|           108248.3|             863|
|d6a260b9099aabdac...|           47333.72|             663|
|a720b2e32df79c52f...|            1904.09|             631|
|a7028bc2602104e65...|  6222.780000000001|             600|
|ea99d0493c7c66889...|  917.4199999999998|             538|
|132141bfb266d8dee...| 51948.170000000006|             536|
|31c9862995b9f65b0...|            2425.02|             499|
|2450ee51be5cb2443...| 17077.760000000002|             476|
|c5582cb6dc5d45a7f...|  555.6999999999999|             474|
|bd89d8f499ff23ce7...|  572.4699999999999|             472|
+--------------------+-------------------+----------------+
only showing top 10 rows

'''

	#Projects having Highest Total Amounts Funded 
project_desc.groupby('ProjectID').agg({'ProjectID':'count','DonationAmount':'sum'}).orderBy('sum(DonationAmount)',ascending=False).show(5)
'''
+--------------------+-------------------+----------------+                     
|           ProjectID|sum(DonationAmount)|count(ProjectID)|
+--------------------+-------------------+----------------+
|c34218abf3fecd36b...|           108248.3|             863|
|8c88fe4f090a65686...|           89511.94|             171|
|bbbc6cccccd3c1bc8...|           70734.47|             359|
|6f96c1fa41246a9e8...|  62289.52999999999|             315|
|132141bfb266d8dee...| 51948.170000000006|             536|
+--------------------+-------------------+----------------+
only showing top 5 rows
'''

	#Maximum Single Donated Amount
project_desc.groupby('ProjectID').agg({'ProjectID':'count','DonationAmount':'max'}).orderBy('max(DonationAmount)',ascending=False).show(5)
'''
+--------------------+-------------------+----------------+                     
|           ProjectID|max(DonationAmount)|count(ProjectID)|
+--------------------+-------------------+----------------+
|8c88fe4f090a65686...|            60000.0|             171|
|8228649f49975197f...|            31856.6|               1|
|6f96c1fa41246a9e8...|           26369.03|             315|
|7c19cb16c571a04e7...|            25000.0|               1|
|822cd8452c30b3888...|           21299.95|               1|
+--------------------+-------------------+----------------+
only showing top 5 rows

'''
	#Highest Average Amounts Per Donor 
tempDF=project_desc.groupby('ProjectID').agg({'ProjectID':'count','DonationAmount':'sum'})
tempDF.withColumn('AvgAm',tempDF['sum(DonationAmount)'] / tempDF['count(ProjectID)']).select('*').filter(tempDF['count(ProjectID)'] > 10).orderBy('AvgAm',ascending=False).show(5)

'''
+--------------------+-------------------+----------------+------------------+  
|           ProjectID|sum(DonationAmount)|count(ProjectID)|             AvgAm|
+--------------------+-------------------+----------------+------------------+
|8638021d087702407...|           33302.58|              18|1850.1433333333334|
|fe3d3889c49b8184e...|           13317.14|              12|1109.7616666666665|
|7cfafabca9ca93af1...|           17027.92|              17|1001.6423529411763|
|9fde8bc007e3a7a37...|           11066.14|              17| 650.9494117647058|
|af7d3a58ba3b8efe8...|  8275.869999999999|              13| 636.6053846153845|
+--------------------+-------------------+----------------+------------------+
only showing top 5 rows
'''

#Who are the Top Donors
	#Donors who have made highest number of donations 
donor_donation.groupby('DonorID').agg({'DonorID':'count','DonationAmount':'sum'}).orderBy('count(DonorID)',ascending=False).show(5)
'''
+--------------------+--------------+-------------------+                       
|             DonorID|count(DonorID)|sum(DonationAmount)|
+--------------------+--------------+-------------------+
|39df9399f5384334a...|         18035| 37121.719999999994|
|237db43817f34988f...|         14565|           84625.62|
|a0e1d358aa17745ff...|         10515| 1879624.9700000002|
|6f74ffb17cbb2b616...|          9029|             9436.0|
|a1929a1172ad0b3d1...|          6427|  28282.86999999999|
+--------------------+--------------+-------------------+
only showing top 5 rows

'''
	#Donors who have funded Highest Total Amounts
donor_donation.groupby('DonorID').agg({'DonorID':'count','DonationAmount':'sum'}).orderBy('sum(DonationAmount)',ascending=False).show(5)
'''
+--------------------+--------------+-------------------+                       
|             DonorID|count(DonorID)|sum(DonationAmount)|
+--------------------+--------------+-------------------+
|a0e1d358aa17745ff...|         10515| 1879624.9700000002|
|2144d56b1947ebb26...|          2152|  1243529.690000001|
|96c4f21513cd8962a...|          2148| 1130565.6199999999|
|3ba8a29e3dd72043f...|          1553|  977614.5300000007|
|f9dd79ea006fee7bb...|          1891|  897408.3100000008|
+--------------------+--------------+-------------------+
only showing top 5 rows
'''

	#Donors who funds Highest Average Amounts in single donations 
tempDF=donor_donation.groupby('DonorID').agg({'DonorID':'count','DonationAmount':'sum'})
tempDF.withColumn('AvgDonAm',tempDF['sum(DonationAmount)'] / tempDF['count(DonorID)']).select('*').orderBy('AvgDonAm',ascending=False).show(5)
'''
+--------------------+--------------+-------------------+--------+              
|             DonorID|count(DonorID)|sum(DonationAmount)|AvgDonAm|
+--------------------+--------------+-------------------+--------+
|b802c941986ae91fd...|             1|            31856.6| 31856.6|
|b51a1b79ce091021e...|             1|            25000.0| 25000.0|
|9a79dcb3a980763ad...|             1|           21295.72|21295.72|
|d4e6cb5bafcf40393...|             1|            15600.0| 15600.0|
|c95f6058c300587dc...|             1|           14198.66|14198.66|
+--------------------+--------------+-------------------+--------+
only showing top 5 rows

'''
	#Donors who have funded Maximum Single Donated Amount 
donor_donation.groupby('DonorID').agg({'DonorID':'count','DonationAmount':'max'}).orderBy('max(DonationAmount)',ascending=False).show(5)
'''
+--------------------+--------------+-------------------+                       
|             DonorID|count(DonorID)|max(DonationAmount)|
+--------------------+--------------+-------------------+
|8f70fc7370842e070...|           223|            60000.0|
|b802c941986ae91fd...|             1|            31856.6|
|46bef03dfe2742079...|            81|           26369.03|
|b51a1b79ce091021e...|             1|            25000.0|
|5aae045f65ce626b6...|             4|           21299.95|
+--------------------+--------------+-------------------+
only showing top 5 rows

'''

#What Donation Amounts were received at what times ? 
	#Donations per Year
	#->Numer of Donations per Year 
	#->average Donation Amounts per Year
from pyspark.sql.functions import year, month, dayofmonth

donation_date=donor_donation.withColumn('DonationYear',year(donor_donation['DonationReceivedDate'])).withColumn('DonationMonth',month(donor_donation['DonationReceivedDate'])).withColumn('DonationDay',dayofmonth(donor_donation['DonationReceivedDate'])).select('*')

donation_date.groupby('DonationYear').agg({'DonationMonth':'count','DonationAmount':'mean'}).orderBy('DonationYear').show()

'''
+------------+-------------------+--------------------+                         
|DonationYear|avg(DonationAmount)|count(DonationMonth)|
+------------+-------------------+--------------------+
|        2012| 166.72912751677853|                 149|
|        2013|   53.0122209968623|              573981|
|        2014|   55.5328932758466|              746607|
|        2015|   64.4508598706601|              783362|
|        2016|  65.44922475850417|              957263|
|        2017|  61.20920621028071|             1190542|
|        2018| 60.802828770500206|              429996|
+------------+-------------------+--------------------+
'''
	#Donations per Month
	#->Number of Donations per Month 
	#->Average Donations per Month
donation_date.groupby('DonationMonth').agg({'DonationYear':'count','DonationAmount':'mean'}).orderBy('DonationMonth').show()
'''
+-------------+-------------------+-------------------+                         
|DonationMonth|count(DonationYear)|avg(DonationAmount)|
+-------------+-------------------+-------------------+
|            1|             411942|  62.81415844949049|
|            2|             396705|   58.7940110913651|
|            3|             465211|  58.67353265507482|
|            4|             332717|  59.95527643613038|
|            5|             237462| 63.787798216135656|
|            6|             191221|  55.10264176005779|
|            7|             286183|   51.6978085001555|
|            8|             577273| 56.042275110736156|
|            9|             518908|  57.27332199542116|
|           10|             408464| 59.552494834306046|
|           11|             400964| 60.570383001965226|
|           12|             454850|  80.17243302187532|
+-------------+-------------------+-------------------+
'''

	#Donations per MonthDay
	#-> Number of Donations per MonthDay
	#-> Average Donations per MonthDay
donation_date.groupby('DonationDay').agg({'DonationYear':'count','DonationAmount':'mean'}).orderBy('DonationDay').show(
'''
+-----------+-------------------+-------------------+                           
|DonationDay|count(DonationYear)|avg(DonationAmount)|
+-----------+-------------------+-------------------+
|          1|             148367| 58.831833089568434|
|          2|             150164|  57.01060014384273|
|          3|             140865| 58.216967593085556|
|          4|             135548|  58.74975469944225|
|          5|             144200|  56.43007142857141|
|          6|             139736| 60.055748411289905|
|          7|             137190|  58.94820708506452|
|          8|             145161|  61.81902721805444|
|          9|             142690| 62.146327843576955|
|         10|             146565|  61.50512079964524|
|         11|             145190|  61.21720366416418|
|         12|             146144|  61.28929535252901|
|         13|             141993| 60.206796109667366|
|         14|             156585| 60.101928728805476|
|         15|             150691| 60.704529401224974|
|         16|             143548| 59.905262211943004|
|         17|             172462| 57.054137143254756|
|         18|             163527|  59.19582283048065|
|         19|             154973| 60.442169926374284|
|         20|             147320|  61.99404615802336|
+-----------+-------------------+-------------------+
only showing top 20 rows
'''
	
#Optional Donations and Teacher Donors 
	#Total Optional Donations and their Associated Average Amounts
donationsDF.groupby('DonationIncludedOptionalDonation').agg({'DonationIncludedOptionalDonation':'count'}).show()

'''
+--------------------------------+-------------------+---------------------------------------+
|DonationIncludedOptionalDonation|avg(DonationAmount)|count(DonationIncludedOptionalDonation)|
+--------------------------------+-------------------+---------------------------------------+
|                              No|  73.61408218020225|                                 686175|
|                             Yes| 58.449052752211465|                                4001709|
+--------------------------------+-------------------+---------------------------------------+
'''

#Total TeacherDonor Donations and the associated Average Amounts
donor_donation.groupby('DonorIsTeacher').agg({'DonorIsTeacher':'count','DonationAmount':'avg'}).show()

'''
+--------------+---------------------+-------------------+                      
|DonorIsTeacher|count(DonorIsTeacher)|avg(DonationAmount)|
+--------------+---------------------+-------------------+
|            No|              3342679|  66.73232292720893|
|           Yes|              1339221|  45.55405142989843|
+--------------+---------------------+-------------------+
'''
#Top States with Maximum Single Donation Amount 
donor_donation.groupby('DonorState').agg({'DonationAmount':'max'}).orderBy('max(DonationAmount)',ascending=False).show(10)
'''
+-----------+-------------------+                                               
| DonorState|max(DonationAmount)|
+-----------+-------------------+
|     Hawaii|            60000.0|
|   Colorado|            31856.6|
| California|           26369.03|
|      Texas|           21299.95|
|    Florida|           21295.72|
|   New York|            20000.0|
|Mississippi|            17777.5|
|   Michigan|            15600.0|
|  Minnesota|           14230.64|
|    Georgia|            14200.0|
+-----------+-------------------+
only showing top 10 rows

'''
#Top Cities with Maximum Single Donation Amount 
donor_donation.groupby('DonorCity').agg({'DonationAmount':'max'}).orderBy('max(DonationAmount)',ascending=False).where(donor_donation.DonorCity != 'null' ).show(10)
'''
+---------------+-------------------+                                           
|      DonorCity|max(DonationAmount)|
+---------------+-------------------+
|        Anahola|            60000.0|
|      Lafayette|            31856.6|
|      Palo Alto|           26369.03|
|    El Sobrante|            25000.0|
|    San Antonio|           21299.95|
|      Rochester|            20000.0|
|       New York|           19588.38|
|      Ridgeland|            17777.5|
|Fort Lauderdale|           17647.32|
|    Los Angeles|           17644.85|
+---------------+-------------------+
only showing top 10 rows
'''

#Growth in Number of Teachers over the years
TeachersDF.groupby(year(TeachersDF.TeacherFirstProjectPostedDate)).agg({'TeacherID':'count'}).orderBy('year(TeacherFirstProjectPostedDate)').show()
'''
+-----------------------------------+----------------+
|year(TeacherFirstProjectPostedDate)|count(TeacherID)|
+-----------------------------------+----------------+
|                               2002|              13|
|                               2003|              52|
|                               2004|             216|
|                               2005|             482|
|                               2006|            1085|
|                               2007|            2223|
|                               2008|            3559|
|                               2009|            5091|
|                               2010|            8483|
|                               2011|           10947|
|                               2012|           17613|
|                               2013|           49268|
|                               2014|           58331|
|                               2015|           62820|
|                               2016|           80431|
|                               2017|           75685|
|                               2018|           26601|
+-----------------------------------+----------------+
'''
