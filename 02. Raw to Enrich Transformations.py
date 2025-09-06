# Databricks notebook source
# MAGIC %md
# MAGIC **Import Depedencies**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Load Data from Bronze(Raw) Source**

# COMMAND ----------

df = (spark.readStream.format("delta")
    .load("abfss://destination@yeasiradls.dfs.core.windows.net/raw_data/"))


# COMMAND ----------

# MAGIC %md
# MAGIC **Explode & Transform Raw Json**

# COMMAND ----------

df1 = df.withColumn("project", explode("employee.projects"))

# COMMAND ----------

df2 = df1.withColumn("task", explode("project.tasks"))

# COMMAND ----------

df3 = df2.withColumn("technology", explode("task.details.technologiesUsed"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Pick Needed Columns**

# COMMAND ----------

df4 = df3.select(
    col("employee.id").alias("employee_id"),
    col("employee.name").alias("employee_name"),
    col("employee.position").alias("employee_position"),
    col("employee.department.id").alias("department_id"),
    col("employee.department.name").alias("department_name"),
    col("employee.department.manager.id").alias("manager_id"),
    col("employee.department.manager.name").alias("manager_name"),
    col("employee.department.manager.contact.email").alias("manager_email"),
    col("employee.department.manager.contact.phone").alias("manager_phone"),
    col("project.projectId").alias("project_id"),
    col("project.projectName").alias("project_name"),
    col("project.startDate").alias("project_startDate"),
    col("task.taskId").alias("task_id"),
    col("task.title").alias("task_title"),
    col("task.status").alias("task_status"),
    col("task.details.expectedCompletion").alias("task_expectedCompletion"),
    col("task.details.hoursSpent").alias("task_hoursSpent"),
    col("technology").alias("technology")
).distinct()



# COMMAND ----------

# MAGIC %md
# MAGIC **Handalling Nulls & Duplicates**

# COMMAND ----------

df_final = df4.withColumn('employee_id', when(col("employee_id").isNull(), "000").otherwise(col("employee_id")))\
    .withColumn('employee_name', when(col("employee_name").isNull(), "UNKNOWN").otherwise(col("employee_name")))\
    .withColumn('employee_position', when(col("employee_position").isNull(), "UNKNOWN").otherwise(col("employee_position")))\
    .withColumn('department_id', when(col("department_id").isNull(), "000").otherwise(col("department_id")))\
    .withColumn('department_name', when(col("department_name").isNull(), "UNKNOWN").otherwise(col("department_name")))\
    .withColumn('manager_id', when(col("manager_id").isNull(), "000").otherwise(col("manager_id")))\
    .withColumn('manager_name', when(col("manager_name").isNull(), "UNKNOWN").otherwise(col("manager_name")))\
    .withColumn('manager_email', when(col("manager_email").isNull(), "UNKNOWN").otherwise(col("manager_email")))\
    .withColumn('manager_phone', when(col("manager_phone").isNull(), "00").otherwise(col("manager_phone")))\
    .withColumn('project_id', when(col("project_id").isNull(), "000").otherwise(col("project_id")))\
    .withColumn('project_name', when(col("project_name").isNull(), "UNKNOWN").otherwise(col("project_name")))\
    .withColumn('project_startDate', when(col("project_startDate").isNull(), "UNKNOWN").otherwise(col("project_startDate")))\
    .withColumn('task_id', when(col("task_id").isNull(), "000").otherwise(col("task_id")))\
    .withColumn('task_title', when(col("task_title").isNull(), "UNKNOWN").otherwise(col("task_title")))\
    .withColumn('task_status', when(col("task_status").isNull(), "UNKNOWN").otherwise(col("task_status")))\
    .withColumn('task_expectedCompletion', when(col("task_expectedCompletion").isNull(), "UNKNOWN").otherwise(col("task_expectedCompletion")))\
    .withColumn('task_hoursSpent', when(col("task_hoursSpent").isNull(), "0").otherwise(col("task_hoursSpent")))\
    .withColumn('technology', when(col("technology").isNull(), "UNKNOWN").otherwise(col("technology")))

# COMMAND ----------

# MAGIC %md
# MAGIC **Write data Into Enrich Layer**

# COMMAND ----------

df_final.writeStream.format("delta")\
    .partitionBy("technology")\
    .option("checkpointLocation", "abfss://destination@yeasiradls.dfs.core.windows.net/checkpoint/enrich_checkpoint/")\
    .outputMode("append")\
    .trigger(once=True)\
    .option("path","abfss://destination@yeasiradls.dfs.core.windows.net/enrich_data/")\
    .start()