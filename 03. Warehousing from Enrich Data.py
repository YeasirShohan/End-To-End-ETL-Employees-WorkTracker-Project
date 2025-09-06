# Databricks notebook source
# MAGIC %md
# MAGIC **Import dependencies**

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Read Data from Enrich Layer**

# COMMAND ----------

df = spark.readStream.format("delta")\
    .load("abfss://destination@yeasiradls.dfs.core.windows.net/enrich_data/")


# COMMAND ----------

# MAGIC %md
# MAGIC **Write Dimension Employee Table**

# COMMAND ----------

df.select("employee_id","employee_name", "employee_position", "department_id").writeStream.format("delta")\
    .option("checkpointLocation", "/Volumes/employee_json_project/warehouse/checkpoint/employee/")\
    .trigger(once=True)\
    .table("employee_json_project.warehouse.dim_employee")

# COMMAND ----------

# MAGIC %md
# MAGIC **Write Dimension Department Table**

# COMMAND ----------

df.select("department_id", "department_name", "manager_id").writeStream.format("delta")\
    .option("checkpointLocation", "/Volumes/employee_json_project/warehouse/checkpoint/department/")\
    .trigger(once=True)\
    .table("employee_json_project.warehouse.dim_department")

# COMMAND ----------

# MAGIC %md
# MAGIC **Write Dimension Manager Table**

# COMMAND ----------

df.select("manager_id", "manager_name", "manager_email", "manager_phone").writeStream.format("delta")\
    .option("checkpointLocation", "/Volumes/employee_json_project/warehouse/checkpoint/manager/")\
    .trigger(once=True)\
    .table("employee_json_project.warehouse.dim_manager")

# COMMAND ----------

# MAGIC %md
# MAGIC **Write Dimension Product Table**

# COMMAND ----------

df.select("project_id", "project_name", "project_startDate").writeStream.format("delta")\
    .option("checkpointLocation", "/Volumes/employee_json_project/warehouse/checkpoint/project/")\
    .trigger(once=True)\
    .table("employee_json_project.warehouse.dim_project")

# COMMAND ----------

df.select("task_id", "task_title", "task_status", "task_expectedCompletion", "task_hoursSpent", "employee_id", "project_id", "department_id", "manager_id").writeStream.format("delta")\
    .option("checkpointLocation", "/Volumes/employee_json_project/warehouse/checkpoint/task/")\
    .trigger(once=True)\
    .table("employee_json_project.warehouse.dim_task")

# COMMAND ----------

df.select("task_id", "technology").writeStream.format("delta")\
    .option("checkpointLocation", "/Volumes/employee_json_project/warehouse/checkpoint/bridge_task_tech/")\
    .trigger(once=True)\
    .table("employee_json_project.warehouse.dim_bridge_task_tech")