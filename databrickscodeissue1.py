# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
data2 = [("James","201017"),
    ("Michael","201117")
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("DateInString",StringType(),True) \
    
  ])
df = spark.createDataFrame(data=data2,schema=schema)
df.printSchema()
df.show(truncate=False) 


# COMMAND ----------

df.createOrReplaceTempView("vwdf")

# COMMAND ----------

# MAGIC %sql
# MAGIC select firstname,
# MAGIC cast(DateInString as date) as StringToDate
# MAGIC from vwdf
