# Databricks notebook source
# MAGIC %run /Workspace/Shared/util/storage_account_access 

# COMMAND ----------

from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import uuid
import datetime
import json

spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.selfDestruct.enabled", "true")

now = datetime.datetime.now()
date_str = now.strftime("%Y/%m/%d")
crosswalk_table_name = "bronze_alwayson.crosswalk_april2024"
aiq_full_population_table_name = "bronze_alwayson.aiq_full_population_april24"
aiq_hem_table_name =  "bronze_alwayson.aiq_full_population_hem_april24"



# COMMAND ----------

# DBTITLE 1,y

@F.udf(returnType=StringType())
def get_id_value(s: str, i: int)-> str:
    tmp = ""
    if s:
        l = s.split("_")
        tmp = l[i]
    return tmp


@F.udf(returnType=StringType())
def get_house_number(s: str) -> str:
    tmp = None
    if s and "PO BOX" not in s:
        l = s.split(" ")
        if l and len(l) > 0:
            house_number: str = l[0]
            if house_number.isnumeric():
                tmp = house_number
    return tmp

@F.udf(returnType=StringType())
def get_house_direction(s: str) -> str:
    tmp = None
    compass = broadcast_compass.value
    if s and "PO BOX" not in s:
        l = s.split(" ")
        if l and len(l) > 0:
            direction = l[1]
            if len(direction) == 1 and direction in compass:
                tmp = direction
    return tmp

@F.udf(returnType=StringType())
def get_location_type(s: str) -> str:
    tmp = None
    if s and "PO BOX" not in s:
        l = []
        for x in s.split(" "):
            if x in broadcast_street_types.value:
                l.append(x)
        if l:
            tmp = l[-1]
    return tmp

@F.udf(returnType=StringType())
def get_street_name(s: str) -> str:
    tmp = None
    if s and "PO BOX" not in s:
        l = []
        for x in s.split(" "):
            if x in broadcast_street_types.value:
                l.append(x)
        if l:
            tmp = l[-1]
    return tmp

broadcast_compass = spark.sparkContext.broadcast(["N", "S", "E", "W"])

with open("./street-suffix-abbreviations.json") as file:
    d = json.load(file)

l = []
for k,v in d.items():
    l.append(k)
    l.extend(v)

broadcast_street_types = spark.sparkContext.broadcast(l)

aiq_full_population_df = spark.sql(f"Select * from {aiq_full_population_table_name}")
aiq_fp_hem_df = spark.sql(f"Select * from {aiq_hem_table_name}")

aiq_fp_hem_df = aiq_fp_hem_df.withColumn("AIQ_HHID", get_id_value(F.col("Customer Unique ID"), F.lit(0)))\
    .withColumn("AIQ_INDID", get_id_value(F.col("Customer Unique ID"), F.lit(1))) \
    .withColumnRenamed("Email Address Array", "email_address_array")\
    .withColumnRenamed("Customer Unique ID", "cuid")
    
join_df = aiq_full_population_df.alias("a").join(
    aiq_fp_hem_df.alias("b"),
    ((F.col("a.AIQ_HHID") == F.col("b.AIQ_HHID")) & (F.col("a.AIQ_INDID") == F.col("b.AIQ_INDID"))),
    "left"
).select("a.*", "b.email_address_array", "b.cuid")

crosswalk_df = join_df.select(
    F.lit("01").alias("Version Number"),
    F.col("cuid").alias("Customer Unique ID"),
    F.lit(None).alias("Unparsed Name"),
    F.lit(None).alias("Unparsed Name Format"),
    F.lit(None).alias("Prefix"),
    F.upper(F.col("Person_First_Name")).alias("First Name"),
    F.upper(F.col("Person_Middle_Initial")).alias("Middle Name"),
    F.upper(F.col("Person_Last_Name")).alias("Last Name"),
    F.upper(F.col("Person_Surname_Suffix")).alias("Generational Suffix"),
    get_house_number(F.upper(F.col("Primary_Address"))).alias("House Number"),
    get_house_direction(F.upper(F.col("Primary_Address"))).alias("Pre Directional"),
    F.lit(None).alias("Street Name"),
    get_location_type(F.upper(F.col("Primary_Address"))).alias("Street Type"),
    F.lit(None).alias("Post Directional"),
    F.lit(None).alias("Unit Type"),
    F.lit(None).alias("Unit Number"),
    F.upper(F.col("Primary_Address")).alias("treet Address Line 1"),
    F.upper(F.col("Secondary_Address")).alias("treet Address Line 2"),
    F.upper(F.col("city")).alias("City"),
    F.upper(F.col("state")).alias("State"),
    F.upper(F.col("Zip5")).alias("ZIP Code"),
    F.col("Zip4").alias("ZIP+4 Code"),
    F.lit(None).alias("Delivery Point Barcode"),
    F.when(
        (F.col("Zip5").isNotNull() & F.col("Zip4").isNotNull()),
        F.concat(F.col("Zip5"), F.col("Zip4"))
    ).otherwise(F.lit(None)).alias("ZIP11 Code"),
    F.lit(None).alias("Phone Number Array"),
    F.when(
        (F.col("area_cd").isNotNull() & F.col("Phone_number").isNotNull()),
        F.concat(F.col("area_cd"), F.col("Phone_number"))
    ).otherwise(F.lit(None)).alias("Phone Number One"),
    F.lit(None).alias("Phone Number Two"),
    F.lit(None).alias("Phone Number Three"),
    F.col("email_address_array").alias("Email Address Array"),
    F.lit(None).alias("Email Address One"),
    F.lit(None).alias("Email Address Two"),
    F.lit(None).alias("Email Address Three"),
    F.lit(None).alias("Mobile Advertising ID (MAID) and MAID Type Array"),
    F.lit(None).alias("Mobile Advertising ID (MAID) One"), 
    F.lit(None).alias("MAID Device Type One"),
    F.lit(None).alias("Mobile Advertising ID (MAID) Two"),
    F.lit(None).alias("MAID Device Type Two"),
    F.lit(None).alias("Mobile Advertising ID (MAID) Three"),
    F.lit(None).alias("MAID Device Type Three"),
    F.lit(None).alias("IP Address and IP Address Time Stamp Array"),
    F.lit(None).alias("IP Address One"),
    F.lit(None).alias("IP Address Time Stamp One"),
    F.lit(None).alias("IP Address Two"),
    F.lit(None).alias("IP Address Time Stamp Two"),
    F.lit(None).alias("IP Address Three"),
    F.lit(None).alias("IP Address Time Stamp Three"),
    F.col("LATITUDE").alias("Latitude"),
    F.col("LONGITUDE").alias("Longitude"),
    F.lit(None).alias("Unparsed Date of Birth"),
    F.col("AIQ_BIRTH_YEAR").alias("Birth Year"),
    F.col("AIQ_BIRTH_MONTH").alias("Birth Month"),
    F.lit(None).alias("Birth Day"),
    F.when(F.col("AIQ_GENDER") == "Male", F.lit("M"))
    .when(F.col("AIQ_GENDER") == "Female", F.lit("F"))
    .otherwise(F.lit("U")).alias("Gender"),
    F.lit(None).alias("TransUnion External ID"),
)
display(crosswalk_df)

