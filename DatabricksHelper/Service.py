from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobTaskSettings, NotebookTask,NotebookTaskSource
from pyspark.sql.functions import explode, col, lit, count, max
from pyspark.sql.types import StructType, StructField, StringType,IntegerType
from pyspark.sql import DataFrame, Observation
from typing import Callable
from pyspark.sql.streaming import StreamingQueryListener, StreamingQuery
from datetime import datetime
import uuid
import json
import os
import time
import shutil
import inspect

class DataReader:
    def __init__(self, data, load_id):
        self.data = data
        self.load_id = load_id

class PipelineService:
  def delete_all_files_and_folders(self, directory_path):
    if os.path.isdir(directory_path):
      for filename in os.listdir(directory_path):
          file_path = os.path.join(directory_path, filename)
          if os.path.isfile(file_path) or os.path.islink(file_path):
              os.unlink(file_path)
          elif os.path.isdir(file_path):
              shutil.rmtree(file_path)
  
  def deep_dict(self, obj):
    if not hasattr(obj, '__dict__'):
        return obj
    result = {}
    for key, val in obj.__dict__.items():
        if key.startswith('_'):
            continue
        element = []
        if isinstance(val, list):
            for item in val:
                element.append(self.deep_dict(item))
        else:
            element = self.deep_dict(val)
        result[key] = element
    return result

  def init_dbutils(self):
    self.databricks_dbutils = None
    if self.spark_session.conf.get("spark.databricks.service.client.enabled") == "true":
      from pyspark.dbutils import DBUtils
      self.databricks_dbutils = DBUtils(self.spark_session)
    else:
      import IPython
      self.databricks_dbutils = IPython.get_ipython().user_ns["dbutils"]

  def __init__(self, spark):
    self.session_id = str(uuid.uuid4())
    self.spark_session = spark
    #self.databricks_dbutils = dbutils
    self.init_dbutils()
    with open(os.environ.get("DATABRICKS_CONFIG"), 'r') as file:
        self.config = json.load(file)
    self.host = spark.conf.get("spark.databricks.workspaceUrl")
    self.workspace_id = self.host.split('.', 1)[0]
    self.workspace = WorkspaceClient(host=self.host, token=self.databricks_dbutils.secrets.get(scope=self.config["Workspace"]["Token"]["Scope"], key=self.config["Workspace"]["Token"]["Secret"]))

class LogService(PipelineService):
  def log(self, category, content, flush = False):
    if isinstance(content, str):
      content = json.loads(content)
    content["log_time"] = str(datetime.now())
    content["session_id"] = self.session_id
    self.logs.setdefault(category, []).append(content)
    if flush:
      self.flush_log()

  def flush_log(self):
      for key, value in self.logs.items():
        log_dir = os.path.join(self.config["Log"]["Path"], key, f"{datetime.now().strftime('%Y%m')}")
        if not os.path.exists(log_dir):
          os.makedirs(log_dir)
        log_file = os.path.join(log_dir, f"{f'{time.time():0<{18}}'.replace('.', '')}-{self.session_id.replace('-', '')}-{str(uuid.uuid4()).replace('-', '')}.json")
        with open(log_file, 'a') as file:
            file.write(json.dumps(value))
      self.logs = {}

  def query_log(self, category, month):
    log_dir = os.path.join(self.config["Log"]["Path"], category, f"{month}")
    if os.path.exists(log_dir):
      return self.spark_session.read.format("json").load(log_dir)

  def job_log(self, month):
    return self.query_log("JobRuns", month)
  
  def ops_log(self, month):
    return self.query_log("Operations", month)
  
  def loader_log(self, month):
    return self.query_log("StreamingProgress", month)
  
  def tick_log(self, month):
    return self.query_log("Ticks", month)
  
  def tick(self, func, flush = False):
    self.log("Ticks", { "func":func }, flush)

  def __init__(self, spark):
    super().__init__(spark)
    self.logs = {}

class StreamingListener(StreamingQueryListener, LogService):
  def onQueryStarted(self, event):
    print("stream got started!")

  def onQueryProgress(self, event):
    print(event.progress.json)
    self.log('StreamingProgress', event.progress.json, True)
    print(self.log)
    row = event.progress.observedMetrics.get("metrics")
    if row is not None:
      print(f"{row.load_id}-{row.cnt} rows processed!")

  def onQueryTerminated(self, event):
    print(f"stream got terminated!")

  def deep_dict(self, obj):
    if not hasattr(obj, '__dict__'):
        return obj
    result = {}
    for key, val in obj.__dict__.items():
        if key.startswith('_'):
            continue
        element = []
        if isinstance(val, list):
            for item in val:
                element.append(self.deep_dict(item))
        else:
            element = self.deep_dict(val)
        result[key] = element
    return result
  
  def __init__(self, spark):
    LogService.__init__(self, spark)

class Pipeline(LogService):
  streaming_listener = None

  def run(self, run_id:str, job_name:str, params = None):
    if params is not None and isinstance(params, str):
      params = json.loads(params)
      
    job_id = self.get_job_id(job_name)
    run = self.workspace.jobs.run_now_and_wait(job_id, notebook_params=params)
    
    result_dict = self.deep_dict(run)
    result_dict["run_id"] = run_id
    self.log("JobRuns", result_dict)
    self.log("Operations", { "Content": f'run job:{job_name}({job_id})' })
    self.flush_log()
    print(f"{json.dumps(result_dict)}\n")
    return run
  
  def get_job_id(self, job_name:str) -> int:
    for job in self.workspace.jobs.list():
      if job_name == job.settings.name:
        return job.job_id
    return -1

  def read_data(self, target_table, source_file, file_format):
    checkpoint_dir = os.path.join(self.config["Data"]["Checkpoint"]["Path"], target_table)
    load_id = str(uuid.uuid4())
    df = self.spark_session.readStream \
      .format("cloudFiles") \
      .option("cloudFiles.format", file_format)\
      .option("cloudFiles.inferColumnTypes", "true")\
      .option("cloudFiles.schemaLocation", checkpoint_dir)\
      .load(source_file) \
      .withColumn("source_metadata",col("_metadata")) \
      .withColumn("load_id",lit(load_id))
    reader = DataReader(df, load_id)
    return reader

  def load_data(self, target_table, source_file, file_format, transform = None, reload_table = 0):
    checkpoint_dir = os.path.join(self.config["Data"]["Checkpoint"]["Path"], target_table)
    if reload_table > 0:
      self.clear_checkpoint(target_table)
      self.log('Operations', { "Content": f'clear checkpoint:{checkpoint_dir}' })
      if not self.truncate_table(target_table, reload_table):
        self.log('Operations', { "Content": f'clear table:{target_table} not exists' })
      else:
        self.log('Operations', { "Content": f'clear table:{target_table}' })
    #spark.streams.addListener(Listener())

    reader = self.read_data(target_table, source_file, file_format)
    df = reader.data
    load_id = reader.load_id
      #.selectExpr("*", "_metadata as source_metadata")
    #:Callable[[DataFrame], DataFrame]
    read_transform = None
    write_transform = None
    if isinstance(transform, list):
      if len(transform) > 0:
        read_transform = transform[0]
      if len(transform) > 1:
        write_transform = transform[1]
    else:
      read_transform = transform
  
    if read_transform is not None and callable(read_transform) and len(inspect.signature(read_transform).parameters) == 1:
      df = read_transform(df)
    df = df.observe("metrics", count(lit(1)).alias("cnt"), max(lit(load_id)).alias("load_id"))
    df = df.writeStream
    if write_transform is not None and callable(write_transform) and len(inspect.signature(write_transform).parameters) == 1:
      df = write_transform(df)
    df = df.partitionBy("load_id")
    df.option("checkpointLocation", checkpoint_dir)\
    .trigger(availableNow=True)\
    .toTable(target_table)

    self.spark_session.conf.set("pileine.load_id", load_id)
    self.log('Operations', { "Content": f'load table:{target_table}' })
    self.flush_log()
    self.wait_loading_data()
    return load_id

  def wait_loading_data(self):
    while len(self.spark_session.streams.active) > 0:
      self.spark_session.streams.resetTerminated() # Otherwise awaitAnyTermination() will return immediately after first stream has terminated
      self.spark_session.streams.awaitAnyTermination()
      time.sleep(0.1)

  def truncate_table(self, table, clear_type):
    try:
        self.spark_session.sql(f"SELECT 1 FROM {table} LIMIT 1")
        if clear_type == 1:
          self.spark_session.sql(f'truncate table {table}')
        elif clear_type == 2:
          self.spark_session.sql(f'drop table {table}')
        return True
    except:
        return False

  def clear_checkpoint(self, table):
    checkpoint_dir = os.path.join(self.config["Data"]["Checkpoint"]["Path"], table)
    self.delete_all_files_and_folders(checkpoint_dir)

  def __init__(self, spark):
    super().__init__(spark)
    if Pipeline.streaming_listener is None:
      Pipeline.streaming_listener = StreamingListener(spark)
      spark.streams.addListener(Pipeline.streaming_listener)
      print(f"add {Pipeline.streaming_listener} {spark.sparkContext.appName}")



# if 'streaming_listeners' not in globals():
#   print("not")
#   streaming_listeners = []
# else:
#   print("yes")


# if len(streaming_listeners) > 0:
#   for listener in streaming_listeners:
#     spark.streams.removeListener(listener)

# listener = StreamingListener(spark, dbutils)
# streaming_listeners.append(listener)
# spark.streams.addListener(listener)


