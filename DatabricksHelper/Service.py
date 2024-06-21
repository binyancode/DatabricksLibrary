from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterDetails
from databricks.sdk.service.jobs import Task, Job, RunNowResponse, Wait, RunResultState, RunLifeCycleState, RunType, ListRunsRunType, Library, JobCluster, JobSettings, JobTaskSettings, NotebookTask,NotebookTaskSource
from pyspark.sql.functions import explode, col, lit, count, max, from_json
from pyspark.sql.types import StructType, StructField, StringType,IntegerType
from pyspark.sql import DataFrame, Observation, SparkSession
from typing import Callable
from pyspark.sql.streaming import StreamingQueryListener, StreamingQuery
from pyspark.sql.streaming.listener import QueryProgressEvent, StreamingQueryProgress
from datetime import datetime
from urllib.parse import unquote
from enum import IntFlag
from types import SimpleNamespace
from types import MethodType
from typing import Callable, Optional
import uuid
import json
import os
import time
import shutil
import inspect
import re
import threading
import requests

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

class PipelineAPI:
    def request(self, method, path, data = None, headers = None):
        if headers is None:
            headers = {}
        if not isinstance(data, str):
            data = json.dumps(data)
        headers["Authorization"] = f"Bearer {self.token}"
        headers["Content-Type"] = "application/json"
        response = requests.request(method, f"{self.host}{path}", headers=headers, data=data)
        return response.json()

    def get(self, path, data = None, headers = None):
        return self.request("GET", path, data, headers)

    def post(self, path, data, headers = None):
        return self.request("POST", path, data, headers = headers)

    def get_job_run(self, run_id, api_version = "2.1"):
        return self.get(f"/api/{api_version}/jobs/runs/get", data={"run_id":run_id, "include_history":True})

    def __init__(self, host, token, protocol = "https") -> None:
        self.host = f"{protocol}://{host}"
        self.token = token

class PipelineService:
    def _delete_all_files_and_folders(self, directory_path):
        if os.path.isdir(directory_path):
            for filename in os.listdir(directory_path):
                    file_path = os.path.join(directory_path, filename)
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
  
    def _deep_dict(self, obj):
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
                element = self._deep_dict(val)
            result[key] = element
        return result
    
    def _copy_object(self, obj, seen=None):
        if seen is None:
            seen = set()

        if id(obj) in seen:
            return

        seen.add(id(obj))


        result = {}
        if isinstance(obj, dict):
            return self._deep_dict(obj)
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
                    element.append(self._copy_object(item, seen))
                result[key] = element
            else:
                #print("object:", key, val)
                result[key] = self._copy_object(val, seen)
        return result
    
    def _init_databricks(self):
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

    def _init_cluster(self):
        cluster_id = self.databricks_dbutils.notebook.entry_point.getDbutils().notebook().getContext().clusterId().get()
        self.cluster = self.workspace.clusters.get(cluster_id)

    def _init_job_task(self):
        if "currentRunId" in self.context:
            task_run_id = self.context["currentRunId"]
            if task_run_id != "" and task_run_id is not None:
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
                self.task = None

    spark_session:SparkSession
    cluster:ClusterDetails
    host:str
    workspace_id:str
    workspace:WorkspaceClient
    api:PipelineAPI
    context:dict
    job:Job
    task:Task

    def __init__(self, spark:SparkSession = None):
        self.session_id:str = str(uuid.uuid4())
        self.spark_session:SparkSession = spark
        #self.databricks_dbutils = dbutils
        self._init_databricks()
        with open(os.environ.get("DATABRICKS_CONFIG"), 'r') as file:
            self.config = json.load(file)
        self.host = self.spark_session.conf.get("spark.databricks.workspaceUrl")
        self.workspace_id = self.host.split('.', 1)[0]
        token = self.databricks_dbutils.secrets.get(scope=self.config["Workspace"]["Token"]["Scope"], key=self.config["Workspace"]["Token"]["Secret"])
        self.workspace = WorkspaceClient(host=self.host, token=token)
        self.api = PipelineAPI(host=self.host, token=token)
        context = json.loads(self.databricks_dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson())
        
        # run_wait = self.workspace.jobs.repair_run(run_id=0, latest_repair_id=0, rerun_all_failed_tasks=True)
        # run_wait.result()
        # {
        #     "attributes": {
        #         "non_uc_api_token": "[REDACTED]",
        #         "notebook_path": "/empty",
        #         "multitaskParentRunId": "717518711058670",
        #         "notebook_id": "4254686541846655",
        #         "orgId": "732672050507723",
        #         "jobType": "NORMAL",
        #         "clusterId": "0528-080821-9r5a98n9",
        #         "idInJob": "654659734638369",
        #         "api_url": "https://chinanorth2.databricks.azure.cn",
        #         "jobId": "159471864380028",
        #         "aclPathOfAclRoot": "/jobs/159471864380028",
        #         "api_token": "[REDACTED]",
        #         "jobTaskType": "notebook",
        #         "jobGroup": "1387871490671876446_9144098961398839279_job-159471864380028-run-654659734638369-action-5728650100615323",
        #         "user": "binyan.ext@zeisscn.partner.onmschina.cn",
        #         "currentRunId": "654659734638369",
        #         "rootRunId": "654659734638369"
        #     }
        # }
        self.context = context["attributes"]
        self._init_job_task()
        # jobGroupId = self.databricks_dbutils.notebook.entry_point.getJobGroupId()
        # print(jobGroupId)
        # match = re.search('run-(.*?)-action', jobGroupId)
        # if match:
        #     task_run_id = match.group(1)
        #     run = self.workspace.jobs.get_run(task_run_id)
        #     try:
        #         self.job = self.workspace.jobs.get(run.job_id)
        #         tasks = [task for task in self.job.settings.tasks if task.task_key == run.run_name]
        #         if len(tasks) > 0:
        #             self.task = tasks[0]
        #         else:
        #             self.task = None
        #     except Exception as ex:
        #         print(f"Cannot found job: {ex}")
        #         self.job = None
        #         self.task = None
        # else:
        #      self.job = None
             


class LogService:
    def log(self, category, content, flush = False):
        if isinstance(content, str):
            content = json.loads(content)
        content["log_time"] = str(datetime.now())
        content["session_id"] = self.session_id
        self.logs.setdefault(category, []).append(content)
        if flush:
            self.flush_log()

    def flush_log(self):
        log_dir = self.log_path
        if not os.path.exists(log_dir):
            try:
                os.makedirs(log_dir)
            except OSError as e:
                print(e)
                pass
        with open(self.log_file, 'w') as file:
            file.write(json.dumps(self.logs, indent=4))

    def get_last_log(self, category, path):
        if category in self.logs:
            if len(self.logs[category]) > 0:
                log = self.logs[category][-1]
                for prop in path:
                    if prop in log:
                        log = log[prop]
                    else:
                        return None
                return log
        return None

    def read_log(self):
        if os.path.exists(self.log_file):
            with open(self.log_file, 'r') as file:
                return json.load(file)
        return {}

    session_id:str
    pipeline_run_id:str
    pipeline_name:str
    log_path:str
    log_file:str
    logs:dict

    def __init__(self, session_id, pipeline_run_id, pipeline_name, log_path):
        self.session_id = session_id
        self.pipeline_run_id = pipeline_run_id
        self.pipeline_name = pipeline_name
        self.log_path = os.path.join(log_path, pipeline_name)
        self.log_file = os.path.join(self.log_path, f"{self.pipeline_run_id}.json")
        self.logs = self.read_log()

class StreamingMetrics:
    row_count:int = 0
    __lock = threading.Lock()
    def reset(self):
        self.row_count = 0
    def add_row_count(self, cnt):
        self.__lock.acquire()
        try:
            # 在这里进行排他性写入
            self.row_count += cnt
        finally:
            # 释放锁
            self.__lock.release()
    def as_dict(self):
        return {"row_count":self.row_count}

class StreamingListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print("stream started!")

    def onQueryProgress(self, event):
        #print(event.progress.json)
        #self.logs.log('streaming_progress', event.progress.json)
        row = event.progress.observedMetrics.get("metrics")
        if row is not None:
            print(f"{row.load_id}-{row.cnt} rows processed!")

            self.metrics.add_row_count(row.cnt)
            print(f"Current row count: {self.metrics.row_count}")
            if self.progress is not None:
                self.progress(self.metrics, event.progress, self.max_load_rows)

    def onQueryTerminated(self, event):
        print(f"stream terminated!")

    logs:LogService
    metrics:StreamingMetrics
    progress:Callable[[StreamingQueryProgress], None]
    max_load_rows:int

    def __init__(self, logs: LogService, metrics: StreamingMetrics, progress:Callable[[StreamingQueryProgress], None] = None, max_load_rows = -1):
        self.logs = logs
        self.metrics = metrics
        self.progress = progress
        self.max_load_rows = max_load_rows

class PipelineCluster(PipelineService):
    def _get_job_id(self, job_name:str) -> int:
        if job_name in self.jobs:
            return self.jobs[job_name]
        for job in self.workspace.jobs.list():
            if job_name == job.settings.name:
                return job.job_id
        return -1
    
    # def max_dop_tasks(self, tasks) -> int:
    #     task_dict = {task['task_key']: task for task in tasks}
    #     root_tasks = [task for task in tasks if not task.get('depends_on')]

    #     max_dop = 0

    #     while root_tasks:
    #         n = len(root_tasks)
    #         if  max_dop < n:
    #             max_dop = n
    #         #max_dop = max(max_dop, n)
    #         for _ in range(n):
    #             task = root_tasks.pop(0)
    #             for dependent_task in tasks:
    #                 if dependent_task.get('depends_on') is not None:
    #                     if any(dependency['task_key'] == task['task_key'] for dependency in dependent_task.get('depends_on')):
    #                         root_tasks.append(dependent_task)

    #     return max_dop
    def __max_dop_tasks(self, tasks) -> int:
        task_dict = {task['task_key']: task for task in tasks}
        root_tasks = [task for task in tasks if not task.get('depends_on')]

        max_dop = 0

        temp_tasks = [root_task["task_key"] for root_task in root_tasks]

        while root_tasks:
            n = len(root_tasks)
            #print(n)
            if  max_dop < n:
                max_dop = n
            #
            # task0
            # └── task5
            #     ├── task6
            #     └── task7

            # task1
            # ├── task2
            # │   ├── task2.1
            # │   └── task4
            # │       └── task4.1
            # └── task3
            #     └── task4
            #         └── task4.1
            #遍历父亲，将父亲替换为孩子，如果孩子有多个父亲，都替换
            # ['task0', 'task1']
            # ['task2', 'task3', 'task5']
            # ['task2.1', 'task4', 'task6', 'task7', 'task4']
            # ['task2.1', 'task6', 'task7', 'task4', 'task4.1']
            # ['task2.1', 'task6', 'task7', 'task4', 'task4.1']
            #

            for temp_task in [temp_task for temp_task in list(set(temp_tasks))]:
                for root_task in root_tasks:
                    depends_on = root_task.get('depends_on')
                    if depends_on and temp_task in [depends_on_key["task_key"] for depends_on_key in depends_on]:

                        #if not root_task["task_key"] in temp_tasks:
                        temp_tasks.append(root_task["task_key"])
                            
                        if temp_task in temp_tasks:
                            temp_tasks.remove(temp_task)
            print(temp_tasks)
            for _ in range(n):
                task = root_tasks.pop(0)
                #print(task["task_key"])
                for dependent_task in tasks:
                    if dependent_task.get('depends_on') is not None:
                        if any(dependency['task_key'] == task['task_key'] for dependency in dependent_task.get('depends_on')):
                            if not any(dependent_task['task_key'] == root_task['task_key'] for root_task in root_tasks):
                                root_tasks.append(dependent_task)
        #temp_tasks = list(set(temp_tasks))
        #print(temp_tasks)
        return len(temp_tasks)
    
    def __get_job_parallel_tasks(self, job_name) -> int:
        job = self.workspace.jobs.get(self._get_job_id(job_name))
        return self.__max_dop_tasks([task.as_dict() for task in job.settings.tasks])

    def __get_current_parallel_tasks(self, cluster_id) -> int:
        max_dop = 0
        for base_run in self.workspace.jobs.list_runs(run_type=ListRunsRunType.JOB_RUN):
            if base_run.state.life_cycle_state not in [RunLifeCycleState.TERMINATED, RunLifeCycleState.INTERNAL_ERROR, RunLifeCycleState.SKIPPED, RunLifeCycleState.TERMINATING]:
                print(base_run.state.life_cycle_state)
                run = self.workspace.jobs.get_run(base_run.run_id)
                #print(json.dumps(run.as_dict()))
                tasks = [task for task in run.tasks if task.state.life_cycle_state not in [RunLifeCycleState.TERMINATED, RunLifeCycleState.INTERNAL_ERROR, RunLifeCycleState.SKIPPED, RunLifeCycleState.TERMINATING] and task.existing_cluster_id == cluster_id]
                # for task in tasks:
                #     print(task.task_key, task.state.life_cycle_state, task.depends_on)
                max_dop += self.__max_dop_tasks([task.as_dict() for task in tasks])
        return max_dop

    def _assign_job_cluster(self, job_name):
        job = None
        for base_job in [base_job for base_job in self.workspace.jobs.list() if base_job.settings.name == job_name]:
            job = self.workspace.jobs.get(base_job.job_id)
        if not job:
            return

        print(f"Entry cluster: {self.cluster.cluster_id}({self.cluster.cluster_name})")

        job_cluster_file = os.path.join(self.config["Job"]["Cluster"]["Path"], self.workspace_id, f"{job_name}.json")
        if os.path.exists(job_cluster_file):
            print(f"The job cluster file '{job_cluster_file}' exists.")
            with open(job_cluster_file, 'r') as file:
                job_cluster_json = json.load(file)

                max_parallel_tasks = self.config["Job"]["Cluster"]["MaxParallelTaskCountForEntryCluster"]


                #判断是否使用当前cluster运算
                if "automatic_cluster_allocation" in job_cluster_json:
                    automatic_cluster_allocation = job_cluster_json["automatic_cluster_allocation"]
                    print(f"Automatic cluster allocation: {automatic_cluster_allocation}")
                    current_parallel_tasks = self.__get_current_parallel_tasks(self.cluster.cluster_id)
                    job_parallel_tasks = self.__get_job_parallel_tasks(job_name)
                    print(f"Max parallel tasks: {max_parallel_tasks}, Current parallel tasks: {current_parallel_tasks}, Job parallel tasks: {job_parallel_tasks}")
                    if automatic_cluster_allocation and max_parallel_tasks >= current_parallel_tasks + job_parallel_tasks:
                        for task in job.settings.tasks:
                            task.existing_cluster_id = self.cluster.cluster_id
                            task.job_cluster_key = None
                            self.workspace.jobs.reset(job.job_id, job.settings)
                            print(f"Job: {job_name} Task: {task.task_key} is allocated: {self.cluster.cluster_id}({self.cluster.cluster_name})")
                        return
                elif not automatic_cluster_allocation:
                    print(f"automatic_cluster_allocation is False.")
                else:
                    print(f"Due to tasks assgined to current cluster: {self.cluster.cluster_id}({self.cluster.cluster_name}) exceed preset maximum degree of parallelism, job: {job_name} is not allocated: {self.cluster.cluster_id}({self.cluster.cluster_name})")
                    
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
                                if existing_cluster_id == "" or existing_cluster_id is None:
                                    existing_cluster_id = self.cluster.cluster_id
                                task.existing_cluster_id = existing_cluster_id
                                self.workspace.jobs.reset(job.job_id, job.settings)
                                print(f"Apply existing cluster {existing_cluster_id} for task: {task.task_key}")
        else:
            print(f"The job cluster file '{job_cluster_file}' does not exist.")
    
    def __init__(self, spark):
        super().__init__(spark)
        self._init_cluster()
        self.jobs = {}
        for job in self.workspace.jobs.list():
            self.jobs[job.settings.name] = job.job_id

class Pipeline(PipelineCluster):
    streaming_listener = None

    def repair_run(self, job_name:str, params = None):
        last_job_run_result = self.logs.get_last_log("job_run_results", ["state", "result_state"])
        last_job_run_id = self.logs.get_last_log("job_run_results", ["run_id"])
        #latest_repair_id = self.logs.get_last_log("job_runs", ["repair_id"])
        latest_repair_id = None
        last_run = self.api.get_job_run(last_job_run_id)
        if "repair_history" in last_run:
            repqirs = [repqir for repqir in last_run["repair_history"] if repqir["type"] == "REPAIR"]
            if repqirs:
                latest_repair_id = repqirs[-1]["id"]

        if last_job_run_id and last_job_run_result in ("FAILED", "TIMEDOUT", "CANCELED"):
            self.logs.log("operations", { "Content": f're-run job:{job_name}' })
            #self.workspace.jobs.get_run(last_job_run_id).overriding_parameters.notebook_params
            
            wait_run = self.workspace.jobs.repair_run(last_job_run_id, latest_repair_id=latest_repair_id, notebook_params=params, rerun_all_failed_tasks=True)
            print(f"Re-running the job {job_name} with status {last_job_run_result}.")

            return self.__running(wait_run)
        else:
            print(f"Skip re-running the job {job_name} with status {last_job_run_result}.")
            return None

    def __running(self, wait_run):
        self.logs.log("job_runs", wait_run.response.as_dict(), True)
        def run_callback(run_return):
            if run_return.state.life_cycle_state != RunLifeCycleState.RUNNING:
                result_dict = run_return.as_dict()
                self.logs.log("job_run_results", result_dict, True)  
        run = wait_run.result(callback=run_callback) 
        print(f"Run job {run.run_name} {run.state.result_state}")
        result_dict = run.as_dict()
        self.logs.log("job_run_results", result_dict)
        self.logs.flush_log()
        print(f"{json.dumps(result_dict)}\n")
        return result_dict

    def run(self, job_name:str, params = None, continue_run = True):
        continue_status = True
        index = 0
        while continue_status:
            index+=1
            run, continue_status = self.run_internal(job_name, params)
            if not continue_run or not continue_status:
                return run
            else:
                print(f"Continue run: {index}")

    def run_internal(self, job_name:str, params = None):
        #print(self.pipeline_cluster.cluster)
        print(f"Current cluster: {self.cluster.cluster_id}({self.cluster.cluster_name})")

        job_id = self._get_job_id(job_name)

        self._assign_job_cluster(job_name)

        if self.logs.get_last_log("job_run_results", ["run_id"]):
            run = self.repair_run(job_name, params)
            if run:
                return (run, False)

        if params is not None and isinstance(params, str):
            params = json.loads(params)
        if self.default_catalog:
            if params is None:
                params = {}
            params["default_catalog"] = self.default_catalog
        self.logs.log("operations", { "Content": f'run job:{job_name}' })
        wait_run = self.workspace.jobs.run_now(job_id, notebook_params=params)
        print(f"Running the job {job_name}.")
        return (self.__running(wait_run), self.__check_continue(job_name))

    def __read_data(self, source_file, file_format, schema_dir, reader_options = None):

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

    def __get_catalog(self, target_table):
        # 使用正则表达式找到所有被``包围的字符串，并将它们替换为无`.`的版本，但保留``
        s_without_dots_in_backticks = re.sub(r'`[^`]*`', lambda x: x.group(0).replace('.', '\u200B'), target_table)
        parts = s_without_dots_in_backticks.split(".")
        if len(parts) == 3:
            first_part = parts[0]
            first_part = first_part.replace('\u200B', '.')
            return first_part.strip('`')
        return self.spark_session.catalog.currentCatalog()

    def __check_continue(self, job_name):
        job = self.workspace.jobs.get(self._get_job_id(job_name))
        for task in job.settings.tasks:
            run_name = os.path.join("task", job.settings.name, task.task_key)
            logs = LogService(self.session_id, self.pipeline_run_id, run_name, self.config["Log"]["Path"])
            if logs.get_last_log("StreamingProgress", ["continue_status"]):
                return True
        return False

    def load_table(self, target_table, source_file, file_format, table_alias = None, reader_options = None, transform = None, reload_table:Reload = Reload.DEFAULT, max_load_rows = -1):
        self.__add_streaming_listener(max_load_rows)
        #target_table = self.spark_session.sql(f'DESC DETAIL {target_table}').first()["name"]
        catalog = self.__get_catalog(target_table) if not self.default_catalog else self.default_catalog
        print(f"Current catalog:{catalog}")

        table_info_dir = os.path.join(catalog, target_table)
        print(f"target table:{target_table}")
        checkpoint_dir = os.path.join(self.config["Data"]["Checkpoint"]["Path"], table_info_dir)
        schema_dir = os.path.join(self.config["Data"]["Schema"]["Path"], table_info_dir)
        if Reload.CLEAR_SCHEMA in reload_table:
            self._delete_all_files_and_folders(schema_dir)
            self.logs.log('operations', { "Content": f'clear schema:{schema_dir}' })
            print(f'clear schema:{schema_dir}')
        if Reload.CLEAR_CHECKPOINT in reload_table:
            self._delete_all_files_and_folders(checkpoint_dir)
            self.logs.log('operations', { "Content": f'clear checkpoint:{checkpoint_dir}' })
            print(f'clear checkpoint:{checkpoint_dir}')
        if Reload.DROP_TABLE in reload_table or Reload.TRUNCATE_TABLE in reload_table:
            print(f'clear table:{target_table}')
            if not self.__truncate_table(target_table, Reload.TRUNCATE_TABLE in reload_table):
                    self.logs.log('operations', { "Content": f'clear table:{target_table} not exists' })
                    print(f'clear table:{target_table} not exists')
            else:
                    self.logs.log('operations', { "Content": f'clear table:{target_table}' })
                    print(f'clear table:{target_table}')
        #spark.streams.addListener(Listener())

        reader = self.__read_data(source_file, file_format, schema_dir, reader_options)
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
        query = df.option("checkpointLocation", checkpoint_dir)\
        .trigger(availableNow=True)\
        .toTable(target_table)
        #query.stop()
        self.__set_task_value("task_load_info", {"table":target_table, "view": table_alias, "load_id":load_id})
        self.logs.log('operations', { "Content": f'load table:{target_table}' })
        self.logs.flush_log()
        self.__wait_loading_data()
        self.logs.flush_log()
        return load_id

    def load_view(self, target_view, target_path, source_file, file_format, view_alias = None, reader_options = None, transform = None, reload_view = False, max_load_rows = -1):
        self.__add_streaming_listener(max_load_rows)
        checkpoint_name = target_path.replace('/', '_')
        checkpoint_dir = os.path.join(self.config["Data"]["Checkpoint"]["Path"], "" if self.default_catalog is None else self.default_catalog, checkpoint_name)
        schema_dir = os.path.join(self.config["Data"]["Schema"]["Path"], "" if self.default_catalog is None else self.default_catalog, checkpoint_name)
        if reload_view:
            self._delete_all_files_and_folders(schema_dir)
            self.logs.log('operations', { "Content": f'clear checkpoint:{schema_dir}' })
            self._delete_all_files_and_folders(checkpoint_dir)
            self.logs.log('operations', { "Content": f'clear checkpoint:{checkpoint_dir}' })
            self._delete_all_files_and_folders(target_path)
            self.logs.log('operations', { "Content": f'clear path:{target_path}' })
        #spark.streams.addListener(Listener())

        reader = self._read_data(checkpoint_name, source_file, file_format, reader_options)
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
        self.__set_task_value("task_load_info", {"table":target_view, "view": view_alias, "load_id":load_id})
        self.logs.log('operations', { "Content": f'load path:{target_path}' })
        self.flush_log()
        self.__wait_loading_data()
        self.view(target_view, target_path, 'delta')
        self.flush_log()
        #self.spark_session.sql(f"CREATE OR REPLACE TEMPORARY VIEW `{target_view}` USING parquet OPTIONS (path '{target_path}')")
        return load_id

    def view(self, view_name, path, file_format = 'parquet'):
        self.spark_session.sql(f"CREATE OR REPLACE VIEW {view_name} as select * from {file_format}.`{path}`")
        #self.spark_session.sql(f"CREATE OR REPLACE VIEW {view_name} USING {file_format} OPTIONS (path '{path}')")


    def __wait_loading_data(self):
        while len(self.spark_session.streams.active) > 0:
            self.spark_session.streams.resetTerminated() # Otherwise awaitAnyTermination() will return immediately after first stream has terminated
            self.spark_session.streams.awaitAnyTermination()
            time.sleep(0.1)

    def __truncate_table(self, table, clear_type):
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

    # def __clear_checkpoint(self, checkpoint_dir):
    #     #checkpoint_dir = os.path.join(self.config["Data"]["Checkpoint"]["Path"], table)
    #     self._delete_all_files_and_folders(checkpoint_dir)

    # def __clear_schema(self, schema_dir):
    #     if 'Schema' in self.config["Data"]:
    #         schema_dir = os.path.join(self.config["Data"]["Schema"]["Path"], table)
    #         self._delete_all_files_and_folders(schema_dir)

    def __set_task_value(self, key, value):
        if self.task and self.job:
            _value = self.databricks_dbutils.jobs.taskValues.get(taskKey = self.task.task_key, key = key, default = "")
            if _value:
                _value = json.loads(_value)
            else:
                _value = {}
            _value = _value.copy()
            _value.update(value)
            self.databricks_dbutils.jobs.taskValues.set(key = key, value = json.dumps(_value))


    def get_load_info(self, schema = None, debug = None, transform = None):
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
                        if transform is not None and temp_view in transform:
                            transform_func = transform[temp_view]
                            if transform_func is not None and callable(transform_func) and len(inspect.signature(transform_func).parameters) == 1:
                                temp_df = transform_func(temp_df)
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
                if transform is not None and temp_view in transform:
                    transform_func = transform[temp_view]
                    if transform_func is not None and callable(transform_func) and len(inspect.signature(transform_func).parameters) == 1:
                        temp_df = transform_func(temp_df)
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

    def finish_load(self, table, state = 'SUCCESS'):
        self.spark_session.sql(f"""
                               CREATE TABLE IF NOT EXISTS {table} (
                                    table_name STRING,
                                    load_id STRING,
                                    load_state STRING,
                                    load_time TIMESTAMP
                                )
                               INSERT INTO {table}
                                SELECT `table`, load_id, '{state}', current_timestamp()
                                FROM load_info
                               """).collect()
        pass

    def clear_table(self, table_names, earlist_time):
        self.logs.log('operations', { "Content": f'clear table:{table_names} older than {earlist_time}' }, True)
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


    def __streaming_progress(self, metrics: StreamingMetrics, progress: StreamingQueryProgress, max_load_rows):
        if max_load_rows < 0:
            return
        if metrics.row_count > max_load_rows:
            if not self.spark_session:
                self._init_databricks()
            for stream in self.spark_session.streams.active:
                #if progress.runId == stream.runId:
                print(f"Current streaming : {progress.id} {progress.runId} ")
                print(f"Stop streaming with row count: {metrics.row_count}")
                print(f"Stop streaming : {stream.id} {stream.runId}")
                self.logs.log("StreamingProgress", {"metrics":metrics.as_dict(), "max_load_rows":max_load_rows, "continue_status":True}, True)
                metrics.reset()
                stream.stop()
        else:
            self.logs.log("StreamingProgress", {"metrics":metrics.as_dict(), "max_load_rows":max_load_rows, "continue_status":False}, True)

    def __add_streaming_listener(self, max_load_rows = -1):
        if Pipeline.streaming_listener is None:
            Pipeline.streaming_listener = StreamingListener(self.logs, self.streaming_metrics, self.__streaming_progress, max_load_rows)
            self.spark_session.streams.addListener(Pipeline.streaming_listener)
            print(f"add {Pipeline.streaming_listener}")

    pipeline_run_id:str
    pipeline_name:str
    default_catalog:str
    logs:LogService
    streaming_metrics:StreamingMetrics

    def __init__(self, pipeline_run_id, default_catalog = None, pipeline_name = None, spark = None):
        super().__init__(spark)

        self.pipeline_run_id = pipeline_run_id
        self.pipeline_name = pipeline_name

        if self.pipeline_name is None:
            if self.job is not None and self.task is not None:
                self.pipeline_name = os.path.join("task", self.job.settings.name, self.task.task_key)
            else:
                self.pipeline_name = os.path.join("notebook", self.context["notebook_path"].strip('/'))
        print(self.pipeline_name)
        self.logs = LogService(self.session_id, pipeline_run_id, self.pipeline_name, self.config["Log"]["Path"])

        print(self.context)
        print(f"Current job: {self.job.settings.name}") if self.job else print("Current job: None")
        print(f"Current task: {self.task.task_key}") if self.task else print("Current task: None")
        self.default_catalog = None
        self.streaming_metrics = StreamingMetrics()
        if default_catalog:
            self.default_catalog = default_catalog
            self.set_default_catalog(self.default_catalog)


__all__ = ['Pipeline', 'Reload']
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


