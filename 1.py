from datetime import datetime

#Import pyspark modules
from pyspark.context import SparkContext
import pyspark.sql.functions as f
from pyspark.sql.functions import lit, concat, when

#Import glue modules
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import json
import boto3
from botocore.exceptions import ClientError
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import functions as sf
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, lit
session = SparkSession.builder.appName("Spark Demo").getOrCreate()

#Initialize contexts and session
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session
s3_source = boto3.resource('s3')

#########################################
### EXTRACT (READ DATA)

# Loading data from both s3 bucket as Data frame 
### Emp DF

df_Emp = session.read.format("csv").option("header", "true").load("s3://htc-source-for-glue/read/emp_updated_city_gender.csv")

print("Showing emp df details ")
df_Emp.show()

### City DF
df_city = session.read.format("csv").option("header", "true").load("s3://htc-poc-glue-source-for-citymap/read/updatedCityNames.csv")

print("Showing city Details")

df_city.show()

## Use Case 1:  Join emp and city DF by mapping  the state and country that is associate with the respective city from the emp df.

## Clean the data set while joining by removing Null and duplicate values
joinDF=df_Emp.join(df_city,df_Emp.city==df_city.CityName, how='left').drop('StateID','city','S.No','CountryCode')

print("Showing  Join DF")
joinDF.show()

### Remove Null values
print("Removing null")
dropNullDF= joinDF.where(sf.col('CityName').isNotNull())
dropNullDF.show()

## Remove Duplicate rows
removeDuplicate=dropNullDF.drop_duplicates(subset=['employeeCode'])
print("Removing duplicate")
removeDuplicate.show()



## Use Case 2: Concatinate 'Full Name' and 'Last Name'

concatDF = removeDuplicate.withColumn('Full Name', sf.concat(sf.col('firstName'),lit(' '),sf.col('lastName')
print("Showing Concat DF")
concatDF.show()
concatDF.printSchema()

### Use case 3: Identify phone numbers those not having country code.
## Function to replace the phone number if its not start with zero
def addCountry_code(phoneNo):
    countryCode= '+00'+phoneNo
    if phoneNo[:1] !='0':
        return str(countryCode)
    else:
        return str(phoneNo)
        
phone_replace_udf=session.udf.register('phone_replace_udf', lambda x: addCountry_code(x), StringType())

phoneNo_rep_DF= concatDF.withColumn("phoneNumber", phone_replace_udf(sf.col('phoneNumber')))

print("Show updated df details")
phoneNo_rep_DF.show()


## Use Case 4: Create Nested json object by including Address field for respective employees. Create respective nested json object for respective countries.
def update_emp_dict_to_Json(empDict):
    key1 = ['userId', 'jobTitleName', 'firstName', 'lastName', 'employeeCode']
    key2 = ['Full Name','gender', 'phoneNumber', 'CityName', 'StateName','CountryName','emailAddress']
    empDetail1={}
    empDetail2={}

    for k in key1:
        empDetail1[k] = empDict[k]
    for k2 in key2:
        empDetail2[k2] = empDict[k2]
        empDetail1.update({'Adress': empDetail2})
    return empDetail1


#Convert the the emp dictionary to the nested json object
def get_Nested_EMp_Json_Object(dataFrame):
    jsonContent = dataFrame.toJSON()
    empArrayObj = []
    for row in jsonContent.collect():
        print(row)
        empDetails= eval(row)
        empArrayObj.append(update_emp_dict_to_Json(empDetails))
        print("Updated nested Emp",json.dumps(empArrayObj))
    return empArrayObj

phoneNo_rep_DF.show()
phoneNo_rep_DF.printSchema()



def getS3Bucket():
    try:
        s3Bucket= s3_source.create_bucket(Bucket='empsource', CreateBucketConfiguration={'LocationConstraint': 'ap-south-1'})
        return s3Bucket
    
    except ClientError as e:
        print("Unable to create bucket",e)





def getOutput_file(country):
    time = datetime.now()
    file= country+"/Employee.json"
    outPutfile= str(time)+file
    return outPutfile








def get_filtered_DataFrame_forRegion(dataFrame):
    ## Filtering based on StateName
    filteredDataframe=[{}]
    countCountry = dataFrame.select('CountryName').distinct()
    listOfcountry=[list(row) for row in countCountry.collect()]
    print("Country list is", listOfcountry)
    try:
        for country in listOfcountry:
            stateFilter_Df=dataFrame.filter(sf.col('CountryName').isin(["".join(country)]))
            print("Showing filtered")
            filtered_Data_json_object=(get_Nested_EMp_Json_Object(stateFilter_Df))
            print("Filtered Data frame for Region", json.dumps(filtered_Data_json_object))
            outputEmpfile= getOutput_file("".join(country))
            s3Bucket=getS3Bucket()
            s3_source.Object('htcpoc-output', outputEmpfile).put(Body=(bytes(json.dumps(filtered_Data_json_object).encode('utf-8-sig'))))

    except AttributeError as e:
        print("Error due to :".e)



get_filtered_DataFrame_forRegion(phoneNo_rep_DF)













