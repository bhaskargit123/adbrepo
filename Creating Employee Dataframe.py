# Databricks notebook source
dbutils.fs.mkdirs("/mnt/adls")

# COMMAND ----------

storageAccountName = "bhaskaradlsgen2"
storageAccountAccessKey = "Iy0PZARd2FGJFK184uee7TIBDWSbcFk0jkoGXMkgZWlpPWHj//eFhS41fSNZ1F6Q0rui8iUSaaTe+AStlNCu/Q=="
storageBlobContainerName = "datafiles"

# COMMAND ----------

dbutils.fs.mount(
	source = "wasbs://{}@{}.blob.core.windows.net".format(storageBlobContainerName, storageAccountName),
  mount_point = "/mnt/adls",
  extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net' : storageAccountAccessKey}
)

# COMMAND ----------

from pyspark.sql.types import * 
schema = StructType(
  [
    StructField('EmployeeNo', IntegerType(), True),
    StructField('Ename', StringType(), True),
    StructField('Job', StringType(), True),
    StructField('HireDate', DateType(), True),
    StructField('Salary', FloatType(), True),
    StructField('Commission', FloatType(), True),
    StructField('DepartmentNo', IntegerType(), True)
  ]
)

# COMMAND ----------

def nullHnadling(x):
  if x=="NULL":
    return None
  return float(x)

# COMMAND ----------

from datetime import datetime
def convert_to_date(x):
  x=datetime.strptime(x, "%Y-%m-%d").date()
  return x

# COMMAND ----------

rdd1 = sc.textFile("/mnt/adls/input/empdata.txt")
rdd2 = rdd1.map(lambda x:x.split(","))
rdd3 = rdd2.map(lambda x:(int(x[0]),x[1],x[2],convert_to_date(x[3]),float(x[4]),nullHnadling(x[5]),int(x[6])))
employeedf = spark.createDataFrame(rdd3, schema)
#rdd3.collect()

# COMMAND ----------

employeedf.createOrReplaceTempView("vemployee")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vemployee
