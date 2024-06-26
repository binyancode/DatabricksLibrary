# Databricks notebook source
from DatabricksHelper.Service import Pipeline,Reload
from DatabricksHelper.ServiceUtils import PipelineUtils
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json

# COMMAND ----------

p_u = PipelineUtils()
params = p_u.init_load_params()
print(params)


#{"cloudFiles.schemaHints":"Data STRING", "cloudFiles.maxFileAge":"1 year", "cloudFiles.partitionColumns":""}
#Reload.DEFAULT|Reload.CLEAR_CHECKPOINT|Reload.CLEAR_SCHEMA|Reload.DROP_TABLE|Reload.TRUNCATE_TABLE
# Reload.DEFAULT = !Reload.CLEAR_CHECKPOINT ,!
#/Volumes/evacatalog/temp/dataonboarding/raw/businesspartner/{year=2024/month=06/day=02, year=2024/month=06/day=05}/

# COMMAND ----------

p = Pipeline(params.pipeline_run_id, params.default_catalog, params.pipeline_name)
load_id = p.load_table(target_table = params.target_table, \
                    source_file = params.source_file, \
                    file_format = params.file_format, \
                    table_alias = "{catalog}_{db}_{table}" if not params.table_alias else params.table_alias, \
                    reader_options = None if not params.reader_options else params.reader_options, \
                    transform = [lambda df:df, lambda df:df], \
                    reload_table = eval("Reload.DEFAULT" if not params.reload_table else params.reload_table), \
                    max_load_rows = params.max_load_rows)

# COMMAND ----------

# task_parameters = p.parse_task_param(task_parameters)
# print(task_parameters)

# COMMAND ----------

#Delete data older than 30 days
try:
    p.clear_table([params.target_table], (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
except Exception as e:
    print(e)
