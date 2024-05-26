from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobTaskSettings, NotebookTask,NotebookTaskSource
from pyspark.sql.functions import explode, col, lit, count, max
from pyspark.sql.types import StructType, StructField, StringType,IntegerType
from pyspark.sql import DataFrame, Observation
from typing import Callable
from pyspark.sql.streaming import StreamingQueryListener, StreamingQuery
from datetime import datetime
from urllib.parse import unquote
import uuid
import json
import os
import time
import shutil
import inspect
import re



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

    def init_databricks(self):
        if self.spark_session is None:
            import IPython
            self.spark_session = IPython.get_ipython().user_ns["spark"]

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
        self.init_databricks()
        with open(os.environ.get("DATABRICKS_CONFIG"), 'r') as file:
            self.config = json.load(file)
        self.host = self.spark_session.conf.get("spark.databricks.workspaceUrl")
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
                    try:
                            os.makedirs(log_dir)
                    except OSError as e:
                            print(e)
                            pass
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
        print("stream started!")

    def onQueryProgress(self, event):
        print(event.progress.json)
        self.log('StreamingProgress', event.progress.json, True)
        #print(self.log)
        row = event.progress.observedMetrics.get("metrics")
        if row is not None:
            print(f"{row.load_id}-{row.cnt} rows processed!")

    def onQueryTerminated(self, event):
        print(f"stream terminated!")

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
        return result_dict
  
    def get_job_id(self, job_name:str) -> int:
        for job in self.workspace.jobs.list():
            if job_name == job.settings.name:
                return job.job_id
        return -1

    def read_data(self, target_table, source_file, file_format, reader_options = None):
        checkpoint_dir = os.path.join(self.config["Data"]["Checkpoint"]["Path"], target_table)
        if 'Schema' in self.config["Data"]:
            schema_dir = os.path.join(self.config["Data"]["Schema"]["Path"], target_table)
        else:
            schema_dir = checkpoint_dir
        load_id = str(uuid.uuid4())
        df = self.spark_session.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", file_format)\
        .option("cloudFiles.inferColumnTypes", "true")\
        .option("cloudFiles.schemaLocation", schema_dir)

        if 'ReaderOptions' in self.config["Data"]:
            for key, value in self.config["Data"]["ReaderOptions"].items():
                df = df.option(key, value)

        if reader_options is not None:
            for key, value in reader_options.items():
                df = df.option(key, value)

        df = df.load(source_file) \
        .withColumn("_source_metadata",col("_metadata")) \
        .withColumn("_load_id",lit(load_id)) \
        .withColumn("_load_time",lit(datetime.now())) 
        reader = DataReader(df, load_id)
        return reader

    def load_table(self, target_table, source_file, file_format, reader_options = None, transform = None, reload_table = 0):
        checkpoint_dir = os.path.join(self.config["Data"]["Checkpoint"]["Path"], target_table)
        schema_dir = os.path.join(self.config["Data"]["Schema"]["Path"], target_table)
        if reload_table & 1:
            self.clear_schema(target_table)
            self.log('Operations', { "Content": f'clear schema:{schema_dir}' })
            print(f'clear schema:{schema_dir}')
        if reload_table & 2:
            self.clear_checkpoint(target_table)
            self.log('Operations', { "Content": f'clear checkpoint:{checkpoint_dir}' })
            print(f'clear checkpoint:{checkpoint_dir}')
        if reload_table & 4:
            print(f'clear table:{target_table}')
            if not self.truncate_table(target_table, reload_table & 8):
                    self.log('Operations', { "Content": f'clear table:{target_table} not exists' })
                    print(f'clear table:{target_table} not exists')
            else:
                    self.log('Operations', { "Content": f'clear table:{target_table}' })
                    print(f'clear table:{target_table}')
        #spark.streams.addListener(Listener())

        reader = self.read_data(target_table, source_file, file_format, reader_options)
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
        print(df.schema)
        df = df.observe("metrics", count(lit(1)).alias("cnt"), max(lit(load_id)).alias("load_id"))
        df = df.writeStream
        if write_transform is not None and callable(write_transform) and len(inspect.signature(write_transform).parameters) == 1:
            df = write_transform(df)
        df = df.partitionBy("_load_id", "_load_time")
        df.option("checkpointLocation", checkpoint_dir)\
        .trigger(availableNow=True)\
        .toTable(target_table)

        self.save_load_id(target_table, load_id)
        self.log('Operations', { "Content": f'load table:{target_table}' })
        self.flush_log()
        self.wait_loading_data()
        return load_id

    def load_view(self, target_view, target_path, source_file, file_format, reader_options = None, transform = None, reload_view = 0):
        checkpoint_name = target_path.replace('/', '_')
        checkpoint_dir = os.path.join(self.config["Data"]["Checkpoint"]["Path"], checkpoint_name)
        schema_dir = os.path.join(self.config["Data"]["Schema"]["Path"], checkpoint_name)
        if reload_view > 0:
            self.clear_schema(checkpoint_name)
            self.log('Operations', { "Content": f'clear checkpoint:{schema_dir}' })
            self.clear_checkpoint(checkpoint_name)
            self.log('Operations', { "Content": f'clear checkpoint:{checkpoint_dir}' })
            self.delete_all_files_and_folders(target_path)
            self.log('Operations', { "Content": f'clear path:{target_path}' })
        #spark.streams.addListener(Listener())

        reader = self.read_data(checkpoint_name, source_file, file_format, reader_options)
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
        df = df.writeStream.format("parquet")
        if write_transform is not None and callable(write_transform) and len(inspect.signature(write_transform).parameters) == 1:
            df = write_transform(df)
        df = df.partitionBy("_load_id", "_load_time")
        df.option("checkpointLocation", checkpoint_dir)\
        .trigger(availableNow=True)\
        .format("delta") \
        .outputMode("append") \
        .option("path", target_path) \
        .start()

        #self.spark_session.conf.set(f"pipeline.{target_view.replace(' ', '_')}.load_id", load_id)
        self.save_load_id(target_view, load_id)
        self.log('Operations', { "Content": f'load path:{target_path}' })
        self.flush_log()
        self.wait_loading_data()
        self.view(target_view, target_path, 'delta')
        #self.spark_session.sql(f"CREATE OR REPLACE TEMPORARY VIEW `{target_view}` USING parquet OPTIONS (path '{target_path}')")
        return load_id

    def view(self, view_name, path, file_format = 'parquet'):
        self.spark_session.sql(f"CREATE OR REPLACE VIEW {view_name} as select * from {file_format}.`{path}`")
        #self.spark_session.sql(f"CREATE OR REPLACE VIEW {view_name} USING {file_format} OPTIONS (path '{path}')")


    def wait_loading_data(self):
        while len(self.spark_session.streams.active) > 0:
            self.spark_session.streams.resetTerminated() # Otherwise awaitAnyTermination() will return immediately after first stream has terminated
            self.spark_session.streams.awaitAnyTermination()
            time.sleep(0.1)

    def truncate_table(self, table, clear_type):
        try:
            self.spark_session.sql(f"SELECT 1 FROM {table} LIMIT 1")
            if clear_type:
                self.spark_session.sql(f'truncate table {table}')
                print(f'truncate table {table}')
            else:
                self.spark_session.sql(f'drop table {table}')
                print(f'drop table {table}')
            return True
        except:
            return False

    def clear_checkpoint(self, table):
        checkpoint_dir = os.path.join(self.config["Data"]["Checkpoint"]["Path"], table)
        self.delete_all_files_and_folders(checkpoint_dir)

    def clear_schema(self, table):
        if 'Schema' in self.config["Data"]:
            schema_dir = os.path.join(self.config["Data"]["Schema"]["Path"], table)
            self.delete_all_files_and_folders(schema_dir)

    def save_load_id(self, table, value):
        key = f"task.{table.replace(' ', '_')}.load_id"
        self.spark_session.conf.set(key, value)
        self.databricks_dbutils.jobs.taskValues.set(key = key, value = value)

    def get_load_id(self, task, table):
        key = f"task.{table.replace(' ', '_')}.load_id"
        load_id = self.databricks_dbutils.jobs.taskValues.get(taskKey = task, key = key, debugValue = "")
        self.spark_session.conf.set(key, load_id)
        return load_id

    def save_load_info(self, table, load_id):
        load_info = {"table":table, "load_id":load_id}
        self.databricks_dbutils.jobs.taskValues.set(key = "task_load_info", value = json.dumps(load_info))

    def get_load_info(self):
        #context = self.databricks_dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        jobGroupId = self.databricks_dbutils.notebook.entry_point.getJobGroupId()
        print(jobGroupId)

        all_load_info = {}
        match = re.search('run-(.*?)-action', jobGroupId)
        if match:
            task_run_id = match.group(1)

            run = self.workspace.jobs.get_run(task_run_id)
            job = self.workspace.jobs.get(run.job_id)
            tasks = [task for task in job.settings.tasks if task.task_key == run.run_name]
            depend_on_task_keys = [dep.task_key for task in tasks for dep in task.depends_on]

            schema = StructType([
                StructField("table", StringType(), True),
                StructField("load_id", StringType(), True)
            ])
            df_load_info = self.spark_session.createDataFrame([], schema)
            df_load_info.createOrReplaceTempView('load_info')

            for task_key in depend_on_task_keys:
                tasks = [task for task in job.settings.tasks if task.task_key == task_key]
                for task in tasks:
                    load_info_value = self.databricks_dbutils.jobs.taskValues.get(taskKey = task.task_key, key = "task_load_info", default = "")
                    print(load_info_value)
                    if load_info_value:
                        load_info = json.loads(load_info_value)
                        all_load_info[load_info["table"]] = load_info
                        df_load_info = df_load_info.union(self.spark_session.createDataFrame([(load_info["table"], load_info["load_id"])], schema))
                        df_load_info.createOrReplaceTempView('load_info')
                        #exec(f'{load_info["table"]}_load_id = "{load_info["load_id"]}"')
                        #spark.createDataFrame([(load_info["table"], load_info["load_id"])], ("table", "load_id")).createOrReplaceTempView('load_info')
                        print(f'{load_info["table"]}_load_id')
                        print(f'{task.task_key}, {load_info["table"]}, {load_info["load_id"]}')
                        
            print(all_load_info)
        else:
            print('No task_run_id found')
        return all_load_info

    def clear_table(self, table_names, earlist_time):
        self.log('Operations', { "Content": f'clear table:{table_names} older than {earlist_time}' })
        for table_name in table_names:
            df = self.spark_session.sql(f"SHOW PARTITIONS {table_name}")
            df.createOrReplaceTempView("partitions")
            self.spark_session.sql(f"delete from {table_name} where _load_id in (select _load_id from partitions where _load_time < '{earlist_time}')").collect()

    def clear_view(self, view_names, earlist_time):
        table_names = []
        for view_name in view_names:
            df = self.spark_session.sql(f"SHOW CREATE TABLE {view_name}").collect()
            df[0].createtab_stmt
            match = re.search('delta.`(.*)`', df[0].createtab_stmt)
            if match:
                    result = match.group(1)
                    table_names.append(f"delta.`{result}`")
            else:
                    print(f"error:invalid view:{view_name}")  # 输出: target
        self.clear_table(table_names, earlist_time)
        # for dirpath, dirnames, filenames in os.walk(view_path):
        #   dirname = os.path.basename(dirpath)
        #   parent_dirname = os.path.basename(os.path.dirname(dirpath))
        #   if dirname.startswith("_load_time"):
        #       if datetime.strptime(unquote(dirname)[11:30], "%Y-%m-%d %H:%M:%S") < datetime.strptime(earlist_time, "%Y-%m-%d"):
        #           print(f"Directory: {unquote(dirname)[11:30]}, Parent directory: {unquote(parent_dirname)}")
        #           self.delete_all_files_and_folders(parent_dirname)

    def __init__(self, spark = None):
        super().__init__(spark)
        if Pipeline.streaming_listener is None:
            Pipeline.streaming_listener = StreamingListener(spark)
            self.spark_session.streams.addListener(Pipeline.streaming_listener)
            print(f"add {Pipeline.streaming_listener} {self.spark_session.sparkContext.appName}")



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


