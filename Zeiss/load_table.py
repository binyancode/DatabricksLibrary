# Databricks notebook source
# MAGIC %md
# MAGIC # 将数据从storage 加载到temp table

# COMMAND ----------

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

p = Pipeline(params.ref_id, params.pipeline_run_id, params.default_catalog, params.pipeline_name)
load_info = p.load_table(target_table = params.target_table, \
                    source_file = params.source_file, \
                    file_format = params.file_format, \
                    table_alias = "{catalog}_{db}_{table}" if not params.table_alias else params.table_alias, \
                    reader_options = None if not params.reader_options else params.reader_options, \
                    column_names = params.column_names, \
                    transform = None if not params.read_transform else params.read_transform, \
                    writer_options = None if not params.writer_options else params.writer_options, \
                    reload_table = eval("Reload.DEFAULT" if not params.reload_table else params.reload_table), \
                    max_load_rows = params.max_load_rows, \
                    task_parameters = params.task_parameters)

# COMMAND ----------

p.wait_loading_data(load_info)

# COMMAND ----------

#Delete data older than 30 days
try:
    p.clear_table([params.target_table], (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
except Exception as e:
    print(e)