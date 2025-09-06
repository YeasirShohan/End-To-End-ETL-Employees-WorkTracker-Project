# Databricks notebook source
# MAGIC %md
# MAGIC **Import dependencies**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Make Source Data Schema**

# COMMAND ----------

schema = StructType([
    StructField("employee", StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("position", StringType()),
        StructField("department", StructType([
            StructField("id", StringType()),
            StructField("name", StringType()),
            StructField("manager", StructType([
                StructField("id", StringType()),
                StructField("name", StringType()),
                StructField("contact", StructType([
                    StructField("email", StringType()),
                    StructField("phone", StringType())
                ]))
            ]))
        ])),
        StructField("projects", ArrayType(StructType([
            StructField("projectId", StringType()),
            StructField("projectName", StringType()),
            StructField("startDate", StringType()),
            StructField("tasks", ArrayType(StructType([
                StructField("taskId", StringType()),
                StructField("title", StringType()),
                StructField("status", StringType()),
                StructField("details", StructType([
                    StructField("completionDate", StringType()),
                    StructField("expectedCompletion", StringType()),
                    StructField("hoursSpent", LongType()),
                    StructField("technologiesUsed", ArrayType(StringType()))
                ]))
            ])))
        ])))
    ]))
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Source Data

# COMMAND ----------

df = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "abfss://destination@yeasiradls.dfs.core.windows.net/schema_location/")
    .option("multiLine", "true")
    .schema(schema)
    .load("abfss://source@yeasiradls.dfs.core.windows.net/sourcedata/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write In Destination

# COMMAND ----------

df.writeStream.format("delta") \
    .option("checkpointLocation", "abfss://destination@yeasiradls.dfs.core.windows.net/checkpoint/raw_checkpoint/") \
    .outputMode("append") \
    .trigger(once=True)\
    .option("path", "abfss://destination@yeasiradls.dfs.core.windows.net/raw_data/") \
    .start()