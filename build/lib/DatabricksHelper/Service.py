from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Library, JobCluster, JobSettings, JobTaskSettings, NotebookTask,NotebookTaskSource
from pyspark.sql.functions import explode, col, lit, count, max, from_json
from pyspark.sql.types import StructType, StructField, StringType,IntegerType
from pyspark.sql import DataFrame, Observation
from typing import Callable
from pyspark.sql.streaming import StreamingQueryListener, StreamingQuery
from datetime import datetime
from urllib.parse import unquote
from enum import IntFlag
from types import SimpleNamespace
from types import MethodType
import uuid
import json
import os
import time
import shutil
import inspect
import re

class Reload(IntFlag):
    DEFAULT = 0
    CLEAR_SCHEMA = 1
    CLEAR_CHECKPOINT = 2
    DROP_TABLE = 4
    TRUNCATE_TABLE = 8

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
    
    def copy_object(self, obj, seen=None):
        if seen is None:
            seen = set()

        if id(obj) in seen:
            return

        seen.add(id(obj))


        result = {}
        if isinstance(obj, dict):
            return self.deep_dict(obj)
        elif not hasattr(obj, '__dir__'):
            return obj
        
        for key in dir(obj):
            if key.startswith('_'):
                continue
            val = getattr(obj, key)
            if 'method' in type(val).__name__:
                continue
            #print(type(val).__name__)
            
            if isinstance(val, (int, float, str, bool, type(None))):
                #print("simple:", key, val)
                result[key] = val
            elif isinstance(val, list):
                #print("list:", key)
                element = []
                for item in val:
                    #print("item:", type(item), key, item)
                    element.append(self.copy_object(item, seen))
                result[key] = element
            else:
                #print("object:", key, val)
                result[key] = self.copy_object(val, seen)
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
        jobGroupId = self.databricks_dbutils.notebook.entry_point.getJobGroupId()
        print(jobGroupId)
        match = re.search('run-(.*?)-action', jobGroupId)
        if match:
            task_run_id = match.group(1)
            run = self.workspace.jobs.get_run(task_run_id)
            try:
                self.job = self.workspace.jobs.get(run.job_id)
                tasks = [task for task in self.job.settings.tasks if task.task_key == run.run_name]
                if len(tasks) > 0:
                    self.task = tasks[0]
                else:
                    self.task = None
            except Exception as ex:
                print(f"Cannot found job: {ex}")
                self.job = None
                self.task = None
        else:
             self.job = None
             


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
        if self.default_catalog:
            if params is None:
                params = {}
            params["default_catalog"] = self.default_catalog
        job_id = self.get_job_id(job_name)
        self.init_job_cluster(job_name)
        run = self.workspace.jobs.run_now_and_wait(job_id, notebook_params=params)
        
        result_dict = self.deep_dict(run)
        result_dict["run_id"] = run_id
        self.log("JobRuns", result_dict)
        self.log("Operations", { "Content": f'run job:{job_name}({job_id})' })
        self.flush_log()
        print(f"{json.dumps(result_dict)}\n")
        return result_dict

    def init_job_cluster(self, job_name):
        job = None
        for base_job in [base_job for base_job in self.workspace.jobs.list() if base_job.settings.name == job_name]:
            job = self.workspace.jobs.get(base_job.job_id)
        if not job:
            return
        
        job_cluster_file = os.path.join(self.config["Job"]["Cluster"]["Path"], self.workspace_id, f"{job_name}.json")
        if os.path.exists(job_cluster_file):
            print(f"The job cluster file '{job_cluster_file}' exists.")
            with open(job_cluster_file, 'r') as file:
                job_cluster_json = json.load(file)

                if "job_clusters" in job_cluster_json:
                    for job_cluster_key, job_cluster_def in job_cluster_json["job_clusters"].items():
                        if job.settings.job_clusters is not None:
                            for cluster in [job_cluster for job_cluster in job.settings.job_clusters if job_cluster.job_cluster_key == job_cluster_key]:
                                job.settings.job_clusters.remove(cluster)
                        else:
                            job.settings.job_clusters = []
                        job.settings.job_clusters.append(JobCluster(job_cluster_key).from_dict(job_cluster_def))
                        self.workspace.jobs.reset(job.job_id, job.settings)
                        print(f"Apply job cluster {job_cluster_key} for job: {job_name}")

                pipeline_library = self.config["Job"]["Cluster"]["PipelineLibaray"]
                
                for task in job.settings.tasks:
                    if "task_job_cluster" in job_cluster_json:
                        for task_key, job_cluster_key in job_cluster_json["task_job_cluster"].items():
                            if task.task_key == task_key:
                                task.job_cluster_key = job_cluster_key
                                if task.libraries is None:
                                    task.libraries = []
                                for library in [library for library in task.libraries if "whl" in library and library["whl"] == pipeline_library]:
                                    task.libraries.remove(library)
                                task.libraries.append({"whl": pipeline_library})
                                self.workspace.jobs.reset(job.job_id, job.settings)
                                print(f"Apply job cluster {job_cluster_key} for task: {task.task_key}")

                    if "task_cluster" in job_cluster_json:
                        for task_key, existing_cluster_id in job_cluster_json["task_cluster"].items():
                            if task.task_key == task_key:
                                task.job_cluster_key = None
                                if task.libraries is None:
                                    task.libraries = []
                                for library in [library for library in task.libraries if "whl" in library and library["whl"] == pipeline_library]:
                                    task.libraries.remove(library)
                                task.libraries.append({"whl":pipeline_library})
                                task.existing_cluster_id = existing_cluster_id
                                self.workspace.jobs.reset(job.job_id, job.settings)
                                print(f"Apply existing cluster {existing_cluster_id} for task: {task.task_key}")
        else:
            print(f"The job cluster file '{job_cluster_file}' does not exist.")

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

    def load_table(self, target_table, source_file, file_format, table_alias = None, reader_options = None, transform = None, reload_table:Reload = Reload.DEFAULT):
        checkpoint_dir = os.path.join(self.config["Data"]["Checkpoint"]["Path"], target_table)
        schema_dir = os.path.join(self.config["Data"]["Schema"]["Path"], target_table)
        if Reload.CLEAR_SCHEMA in reload_table:
            self.clear_schema(target_table)
            self.log('Operations', { "Content": f'clear schema:{schema_dir}' })
            print(f'clear schema:{schema_dir}')
        if Reload.CLEAR_CHECKPOINT in reload_table:
            self.clear_checkpoint(target_table)
            self.log('Operations', { "Content": f'clear checkpoint:{checkpoint_dir}' })
            print(f'clear checkpoint:{checkpoint_dir}')
        if Reload.DROP_TABLE in reload_table or Reload.TRUNCATE_TABLE in reload_table:
            print(f'clear table:{target_table}')
            if not self.truncate_table(target_table, Reload.TRUNCATE_TABLE in reload_table):
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

        self.save_load_info(target_table, table_alias, load_id)
        self.log('Operations', { "Content": f'load table:{target_table}' })
        self.flush_log()
        self.wait_loading_data()
        return load_id

    def load_view(self, target_view, target_path, source_file, file_format, view_alias = None, reader_options = None, transform = None, reload_view = False):
        checkpoint_name = target_path.replace('/', '_')
        checkpoint_dir = os.path.join(self.config["Data"]["Checkpoint"]["Path"], checkpoint_name)
        schema_dir = os.path.join(self.config["Data"]["Schema"]["Path"], checkpoint_name)
        if reload_view:
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
        self.save_load_info(target_view, view_alias, load_id)
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

    def save_load_info(self, table, view, load_id):
        load_info = {"table":table, "view": view, "load_id":load_id}
        self.databricks_dbutils.jobs.taskValues.set(key = "task_load_info", value = json.dumps(load_info))

    def get_load_info(self, schema = None, debug = None):
        #context = self.databricks_dbutils.notebook.entry_point.getDbutils().notebook().getContext()

        all_load_info = {}
        if self.job is not None and self.task is not None:
            depend_on_task_keys = [depend_on.task_key for depend_on in self.task.depends_on]

            load_info_schema = StructType([
                StructField("table", StringType(), True),
                StructField("load_id", StringType(), True)
            ])
            df_load_info = self.spark_session.createDataFrame([], load_info_schema)
            df_load_info.createOrReplaceTempView('load_info')

            for task_key in depend_on_task_keys:
                tasks = [task for task in self.job.settings.tasks if task.task_key == task_key]
                for task in tasks:
                    load_info_value = self.databricks_dbutils.jobs.taskValues.get(taskKey = task.task_key, key = "task_load_info", default = "")
                    print(load_info_value)
                    if load_info_value:
                        load_info = json.loads(load_info_value)
                        df_load_info = df_load_info.union(self.spark_session.createDataFrame([(load_info["table"], load_info["load_id"])], load_info_schema))
                        df_load_info.createOrReplaceTempView('load_info')

                        query = f"""
                            SELECT * 
                            FROM {load_info["table"]} 
                            WHERE _load_id = '{load_info["load_id"]}' 
                            """
                        catalog = ""
                        db = ""
                        table = ""
                        for part in reversed(load_info["table"].split('.')):
                            if not table:
                                    table = part
                            elif not db:
                                db = part
                            else:
                                catalog = part

                        temp_view = f"{catalog}_{db}_{table}"
                        if load_info["view"]:
                            temp_view = eval(f'f"{load_info["view"]}"')
                        temp_df = self.spark_session.sql(query)
                        temp_df.createOrReplaceTempView(temp_view)
                        #load_info["data"] = temp_df
                        all_load_info[f"{temp_view}_info"] = SimpleNamespace(**load_info)
                        all_load_info[temp_view] = temp_df 

                        #exec(f'{load_info["table"]}_load_id = "{load_info["load_id"]}"')
                        #spark.createDataFrame([(load_info["table"], load_info["load_id"])], ("table", "load_id")).createOrReplaceTempView('load_info')
                        print(f'{load_info["table"]}_load_id')
                        print(f'{task.task_key}, {load_info["table"]}, {load_info["load_id"]}')

            for table in [table for table in self.spark_session.catalog.listTables() if table.tableType == "TEMPORARY"]:
                print(f"{table.name} {table.tableType}")
            self.spark_session.sql("select * from load_info").collect()            
            print(all_load_info)
        elif debug is not None:
            print('No task_run_id found, debug mode.')
            for temp_view, temp_table in debug.items():
                query = f"""
                    SELECT t.* FROM {temp_table} t where _load_id = 
                    (
                        SELECT MAX(_load_id)
                        FROM {temp_table}
                        WHERE _load_time = (SELECT MAX(_load_time) FROM {temp_table})
                    )
                    """
                temp_df = self.spark_session.sql(query)
                temp_df.createOrReplaceTempView(temp_view)
                all_load_info[temp_view] = temp_df
        if schema is not None:
            for temp_view, temp_df in all_load_info.items():
                view_schema = schema.get(temp_view)
                if view_schema:
                    temp_df = all_load_info[temp_view]
                    for col_name, col_schema in view_schema.items():
                        temp_df = temp_df.withColumn(col_name, from_json(col(col_name), col_schema))
                        print(f"view:{temp_view} apply schema:{col_name}")
                    temp_df.createOrReplaceTempView(temp_view)
                    all_load_info[temp_view] = temp_df

        load = SimpleNamespace(**all_load_info)
        #load.load = MethodType(lambda this,table:self.load_temp_table(table), load)
        return load

    def set_default_catalog(self, catalog):
        self.spark_session.catalog.setCurrentCatalog(catalog)

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

    def __init__(self, default_catalog = None, spark = None):
        super().__init__(spark)
        self.default_catalog = None
        if default_catalog:
            self.default_catalog = default_catalog
            self.set_default_catalog(self.default_catalog)
        if Pipeline.streaming_listener is None:
            Pipeline.streaming_listener = StreamingListener(spark)
            self.spark_session.streams.addListener(Pipeline.streaming_listener)
            print(f"add {Pipeline.streaming_listener}")



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


