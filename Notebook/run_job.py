# Databricks notebook source
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("job_name", "")
dbutils.widgets.text("job_params", "")

run_id = dbutils.widgets.get("run_id")
job_name = dbutils.widgets.get("job_name")
job_params = dbutils.widgets.get("job_params")

if job_params == "":
    job_params = None

# COMMAND ----------

from DatabricksHelper.Service import Pipeline

p = Pipeline()
p.run(run_id, job_name, job_params)
