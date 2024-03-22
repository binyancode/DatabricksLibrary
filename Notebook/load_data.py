# Databricks notebook source
dbutils.widgets.text("table", "myfirstcatalog.mytestdb.testschema")
dbutils.widgets.text("source", "/Volumes/myfirstcatalog/mytestdb/external-volume/data/testdata/")
dbutils.widgets.text("source_format", "csv")
dbutils.widgets.text("reader_options", "{\"cloudFiles.schemaHints\":\"date DATE\"}")
dbutils.widgets.text("reload_table", "0")

table = dbutils.widgets.get("table")
source = dbutils.widgets.get("source")
source_format = dbutils.widgets.get("source_format")
reader_options = dbutils.widgets.get("reader_options")
reload_table = int(dbutils.widgets.get("reload_table"))
print(reader_options)

# COMMAND ----------

if reload_table & 1:
    print(f'clear schema')
if reload_table & 2:
    print(f'clear checkpoint')
if reload_table & 4:
    print(f'clear table')
if reload_table & 8:
    print(f'truncate table')
else:
    print(f'drop table')

# COMMAND ----------

from DatabricksHelper.Service import Pipeline
p = Pipeline()

# COMMAND ----------

import json
options = json.loads(reader_options)

#load table column type.json
#{ "A" : "a1 INT, a2 DATE, a3 DATE", "B": "" }
#serilize dict
#dict["A"]
#options["cloudFiles.schemaHints"] = dict["A"]
#options["delimier"] = "|"


def sch_remove_whitespace(df):
  for c in df.schema:
    if " " in c.name:
      #if you want to replace the whitespace of column name, enable the next following statement
      #df = df.withColumn(c.name.replace(" ", "_"), col(c.name)).drop(c.name)
      print(c)
  return df

load_id = p.load_table(table, source, source_format, reader_options= options, transform= [sch_remove_whitespace, lambda df:df.option("mergeSchema", "true")], reload_table=reload_table)

# COMMAND ----------

from datetime import datetime, timedelta
#Delete data older than 30 days
p.clear_table([table], (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /Volumes/myfirstcatalog/mytestdb/external-volume/data/testdata/
