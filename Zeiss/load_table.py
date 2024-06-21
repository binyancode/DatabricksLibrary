# Databricks notebook source
from DatabricksHelper.Service import Pipeline,Reload
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json

# COMMAND ----------

dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("default_catalog", "evacatalog")
dbutils.widgets.text("table", "temp.test")
dbutils.widgets.text("source", "/Volumes/evacatalog/temp/dataonboarding/raw/businesspartner/")
dbutils.widgets.text("dynamic_source", "")
dbutils.widgets.text("source_format", "json")
dbutils.widgets.text("table_alias", "{catalog}_{db}_{table}")
dbutils.widgets.text("reader_options", "") #{\"cloudFiles.schemaHints\":\"date DATE\"}
dbutils.widgets.text("reload_table", "Reload.DEFAULT")
dbutils.widgets.text("max_load_rows", "-1")

pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
default_catalog = dbutils.widgets.get("default_catalog")
table = dbutils.widgets.get("table")
source = dbutils.widgets.get("source")
dynamic_source = dbutils.widgets.get("dynamic_source")
source_format = dbutils.widgets.get("source_format")
table_alias = dbutils.widgets.get("table_alias")
reader_options = dbutils.widgets.get("reader_options")
reload_table = dbutils.widgets.get("reload_table")
max_load_rows = dbutils.widgets.get("max_load_rows")
if reader_options:
    reader_options = json.loads(reader_options)
if max_load_rows:
    max_load_rows = int(max_load_rows)
if dynamic_source:
    source = eval(dynamic_source)
print(source)
print(default_catalog)
print(reader_options)
print(reload_table)
#{"cloudFiles.schemaHints":"Data STRING", "cloudFiles.maxFileAge":"1 year", "cloudFiles.partitionColumns":""}
#Reload.DEFAULT|Reload.CLEAR_CHECKPOINT|Reload.CLEAR_SCHEMA|Reload.DROP_TABLE|Reload.TRUNCATE_TABLE
# Reload.DEFAULT = !Reload.CLEAR_CHECKPOINT ,!
#/Volumes/evacatalog/temp/dataonboarding/raw/businesspartner/{year=2024/month=06/day=02, year=2024/month=06/day=05}/

# COMMAND ----------

p = Pipeline(pipeline_run_id, default_catalog)
load_id = p.load_table(table, \
                    source, \
                    source_format, \
                    table_alias = "{catalog}_{db}_{table}" if not table_alias else table_alias, \
                    reader_options= None if not reader_options else reader_options, \
                    transform= [lambda df:df, lambda df:df], \
                    reload_table=eval("Reload.DEFAULT" if not reload_table else reload_table), \
                    max_load_rows = max_load_rows)

# COMMAND ----------

#Delete data older than 30 days
p.clear_table([table], (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
