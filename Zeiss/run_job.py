# Databricks notebook source
from DatabricksHelper.Service import Pipeline
import uuid

# COMMAND ----------

parameter_list = ["pipeline_run_id", "job_name", "default_catalog", "target_table", "source_file", "file_format", "table_alias", "reader_options", "reload_table", "max_load_rows", "continue_run", "task_parameters"]
for parameter in parameter_list:
    eval(f'dbutils.widgets.text("{parameter}", "")')
    exec(f'{parameter} = dbutils.widgets.get("{parameter}")')
    print(f'{parameter}: ', eval(f'{parameter}'))

if continue_run:
    continue_run = bool(continue_run)
else:
    continue_run = True

params = {}
for param in parameter_list:
    value = eval(param)
    if value is not None and value != "":
        params[param] = value
print(params)

# COMMAND ----------

p = Pipeline(pipeline_run_id, default_catalog)
run = p.run(job_name, params, continue_run)

