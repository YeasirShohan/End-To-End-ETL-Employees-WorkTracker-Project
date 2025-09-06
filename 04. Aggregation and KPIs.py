# Databricks notebook source
# MAGIC %md
# MAGIC **Import dependencies**

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Project Progress Summary**

# COMMAND ----------

kpi1_df = spark.sql("""SELECT 
  p.project_id,
  p.project_name,
  COUNT(t.task_id) AS total_tasks,
  SUM(CASE WHEN t.task_status = 'Completed' THEN 1 ELSE 0 END) AS completed_tasks,
  SUM(t.task_hoursSpent) AS total_hours_spent,
  MAX(t.task_expectedCompletion) AS project_due_date
FROM employee_json_project.warehouse.fact_task t
JOIN employee_json_project.warehouse.dim_project p ON p.project_id = t.project_id GROUP BY p.project_id, p.project_name""")


# COMMAND ----------

kpi1_df.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable("employee_json_project.kpis.kpi1")

# COMMAND ----------

# MAGIC %md
# MAGIC **Employee Productivity Summary**

# COMMAND ----------

kpi2_df = spark.sql("""SELECT 
  e.employee_id,
  e.employee_name,
  COUNT(t.task_id) AS tasks_assigned,
  SUM(t.task_hoursSpent) AS total_hours,
  AVG(t.task_hoursSpent) AS avg_hours_per_task
FROM employee_json_project.warehouse.fact_task t
JOIN employee_json_project.warehouse.dim_employee e ON t.employee_id = e.employee_id
GROUP BY e.employee_id, e.employee_name""")


# COMMAND ----------

kpi2_df.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable("employee_json_project.kpis.kpi2")