# Databricks notebook source
from DatabricksHelper.Service import Pipeline,Reload
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json

# COMMAND ----------

parameter_list = ["pipeline_run_id", "default_catalog", "target_table", "source_file", "file_format", "table_alias", "reader_options", "reload_table", "max_load_rows", "task_parameters"]
for parameter in parameter_list:
    eval(f'dbutils.widgets.text("{parameter}", "")')
    exec(f'{parameter} = dbutils.widgets.get("{parameter}")')
    print(f'{parameter}: ', eval(f'{parameter}'))


if reader_options:
    reader_options = json.loads(reader_options)
if max_load_rows:
    max_load_rows = int(max_load_rows)

#{"cloudFiles.schemaHints":"Data STRING", "cloudFiles.maxFileAge":"1 year", "cloudFiles.partitionColumns":""}
#Reload.DEFAULT|Reload.CLEAR_CHECKPOINT|Reload.CLEAR_SCHEMA|Reload.DROP_TABLE|Reload.TRUNCATE_TABLE
# Reload.DEFAULT = !Reload.CLEAR_CHECKPOINT ,!
#/Volumes/evacatalog/temp/dataonboarding/raw/businesspartner/{year=2024/month=06/day=02, year=2024/month=06/day=05}/

# COMMAND ----------

p = Pipeline(pipeline_run_id, default_catalog)
load_id = p.load_table(target_table = target_table, \
                    source_file = source_file, \
                    file_format = file_format, \
                    table_alias = "{catalog}_{db}_{table}" if not table_alias else table_alias, \
                    reader_options = None if not reader_options else reader_options, \
                    transform = [lambda df:df, lambda df:df], \
                    reload_table = eval("Reload.DEFAULT" if not reload_table else reload_table), \
                    max_load_rows = max_load_rows)

# COMMAND ----------

# task_parameters = p.parse_task_param(task_parameters)
# print(task_parameters)

# COMMAND ----------

#Delete data older than 30 days
try:
    p.clear_table([target_table], (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
except Exception as e:
    print(e)
