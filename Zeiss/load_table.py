# Databricks notebook source
from DatabricksHelper.Service import Pipeline,Reload
from datetime import datetime, timedelta
import json

# COMMAND ----------

dbutils.widgets.text("default_catalog", "evacatalog")
dbutils.widgets.text("table", "temp.test")
dbutils.widgets.text("source", "/Volumes/evacatalog/temp/dataonboarding/raw/businesspartner/")
dbutils.widgets.text("source_format", "json")
dbutils.widgets.text("table_alias", "{catalog}_{db}_{table}")
dbutils.widgets.text("reader_options", "") #{\"cloudFiles.schemaHints\":\"date DATE\"}
dbutils.widgets.text("reload_table", "Reload.DEFAULT")

default_catalog = dbutils.widgets.get("default_catalog")
table = dbutils.widgets.get("table")
source = dbutils.widgets.get("source")
source_format = dbutils.widgets.get("source_format")
table_alias = dbutils.widgets.get("table_alias")
reader_options = dbutils.widgets.get("reader_options")
reload_table = dbutils.widgets.get("reload_table")
if reader_options:
    reader_options = json.loads(reader_options)
print(reader_options)
print(reload_table)
#Reload.DEFAULT|Reload.CLEAR_CHECKPOINT|Reload.CLEAR_SCHEMA|Reload.DROP_TABLE|Reload.TRUNCATE_TABLE

# COMMAND ----------

p = Pipeline(default_catalog)
load_id = p.load_table(table, \
                       source, \
                       source_format, \
                       table_alias = "{catalog}_{db}_{table}" if not table_alias else table_alias, \
                       reader_options= None if not reader_options else reader_options, \
                       transform= [lambda df:df, lambda df:df], \
                       reload_table=eval("Reload.DEFAULT" if not reload_table else reload_table))

# COMMAND ----------

#Delete data older than 30 days
p.clear_table([table], (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
