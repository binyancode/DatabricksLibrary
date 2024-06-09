# Databricks notebook source
from DatabricksHelper.Service import Pipeline
import uuid

# COMMAND ----------

dbutils.widgets.text("run_id", str(uuid.uuid4()))
dbutils.widgets.text("job_name", "job1")
dbutils.widgets.text("default_catalog", "")
dbutils.widgets.text("reader_options", "")
dbutils.widgets.text("reload_table", "")
run_id = dbutils.widgets.get("run_id")
job_name = dbutils.widgets.get("job_name")
default_catalog = dbutils.widgets.get("default_catalog")
reader_options = dbutils.widgets.get("reader_options")
reload_table = dbutils.widgets.get("reload_table")
print(default_catalog, job_name, reader_options)

# COMMAND ----------

p = Pipeline(default_catalog)
p.run(run_id, job_name, \
        { \
            "default_catalog":default_catalog, \
            "reader_options":reader_options, \
            "reload_table":reload_table \
        })
