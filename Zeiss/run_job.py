# Databricks notebook source
from DatabricksHelper.Service import Pipeline
import uuid

# COMMAND ----------

dbutils.widgets.text("pipeline_run_id", str(uuid.uuid4()))
dbutils.widgets.text("job_name", "bp_job")
dbutils.widgets.text("default_catalog", "evacatalog")
dbutils.widgets.text("continue_run", "True")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
job_name = dbutils.widgets.get("job_name")
default_catalog = dbutils.widgets.get("default_catalog")
continue_run = dbutils.widgets.get("continue_run")
if continue_run:
    continue_run = bool(continue_run)
print(f"pipeline_run_id:{pipeline_run_id}")
print(default_catalog, job_name)

# COMMAND ----------

p = Pipeline(pipeline_run_id, default_catalog)

run = p.run(job_name, \
            { \
                "pipeline_run_id":pipeline_run_id, \
                "default_catalog":default_catalog \
            }, \
            continue_run)

