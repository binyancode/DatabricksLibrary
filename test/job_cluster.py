from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobCluster, JobSettings, JobTaskSettings, NotebookTask,NotebookTaskSource
import json
from types import SimpleNamespace

class JobSettingsDict(JobSettings):
    def __init__(self, json_str):
        self.json_str = json_str
        json_obj = json.loads(json_str)
        super().__init__(**json_obj)   
    def as_dict(self):
        return self.copy_object(self)
    def __dict__(self):
        return self.copy_object(self)
    def deep_dict(self, obj):
        #print(obj)
        if not hasattr(obj, '__dict__'):
            return obj
        result = {}
        for key, val in obj.__dict__.items():
            #print(key)
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
    
    def recursive_simplenamespace(self, obj):
        if isinstance(obj, dict):
            for key in obj:
                obj[key] = self.recursive_simplenamespace(obj[key])
            return SimpleNamespace(**obj)
        elif isinstance(obj, list):
            return [self.recursive_simplenamespace(item) for item in obj]
        else:
            return obj

class JobClusterDict(JobCluster):
    def __init__(self, json_str):
        self.json_str = json_str
        json_obj = json.loads(json_str)
        super().__init__(**json_obj)   
    def as_dict(self):
        return self.copy_object(self)
    def __dict__(self):
        return self.copy_object(self)
    def deep_dict(self, obj):
        #print(obj)
        if not hasattr(obj, '__dict__'):
            return obj
        result = {}
        for key, val in obj.__dict__.items():
            #print(key)
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
    
    def recursive_simplenamespace(self, obj):
        if isinstance(obj, dict):
            for key in obj:
                obj[key] = self.recursive_simplenamespace(obj[key])
            return SimpleNamespace(**obj)
        elif isinstance(obj, list):
            return [self.recursive_simplenamespace(item) for item in obj]
        else:
            return obj

workspace = WorkspaceClient(host="https://adb-732672050507723.3.databricks.azure.cn/", token="dapi91bc55dd4e482c68198ebb264a9b775a")
for base_job in workspace.jobs.list():
    job = workspace.jobs.get(base_job.job_id)
    
    #workspace.jobs.reset(base_job.job_id,)
    # for job_cluster in job.settings.job_clusters:
    #     job_cluster.new_cluster.node_type_id = "Standard_D4ds_v5"
    json_str = """{
                    "name": "bp_job",
                    "email_notifications": {
                        "no_alert_for_skipped_runs": false
                    },
                    "timeout_seconds": 0,
                    "max_concurrent_runs": 1,
                    "tasks": [
                        {
                        "task_key": "load_bp",
                        "run_if": "ALL_SUCCESS",
                        "notebook_task": {
                            "notebook_path": "/Workspace/eVA/workflow_core/load_table",
                            "base_parameters": {
                            "source": "/Volumes/evacatalog/temp/dataonboarding/raw/businesspartner/",
                            "source_format": "json",
                            "table_alias": "{table}",
                            "reader_options": "",
                            "reload_table": "Reload.DEFAULT",
                            "table": "temp.bp_test"
                            },
                            "source": "WORKSPACE"
                        },
                        "existing_cluster_id": "0528-080821-9r5a98n9",
                        "timeout_seconds": 0,
                        "email_notifications": {},
                        "notification_settings": {
                            "no_alert_for_skipped_runs": false,
                            "no_alert_for_canceled_runs": false,
                            "alert_on_last_attempt": false
                        }
                        },
                        {
                        "task_key": "transform_bp",
                        "depends_on": [
                            {
                            "task_key": "load_bp"
                            }
                        ],
                        "run_if": "ALL_SUCCESS",
                        "notebook_task": {
                            "notebook_path": "/Workspace/eVA/transform/transform_bp",
                            "source": "WORKSPACE"
                        },
                        "existing_cluster_id": "0528-080821-9r5a98n9",
                        "timeout_seconds": 0,
                        "email_notifications": {},
                        "notification_settings": {
                            "no_alert_for_skipped_runs": false,
                            "no_alert_for_canceled_runs": false,
                            "alert_on_last_attempt": false
                        }
                        }
                    ],
                    "job_clusters": [
                        {
                        "job_cluster_key": "Job_cluster001",
                        "new_cluster": {
                            "cluster_name": "",
                            "spark_version": "14.3.x-scala2.12",
                            "azure_attributes": {
                            "first_on_demand": 1,
                            "availability": "ON_DEMAND_AZURE",
                            "spot_bid_max_price": -1
                            },
                            "node_type_id": "Standard_D4ds_v5",
                            "enable_elastic_disk": true,
                            "data_security_mode": "SINGLE_USER",
                            "runtime_engine": "PHOTON",
                            "num_workers": 5
                        }
                        }
                    ],
                    "format": "MULTI_TASK"
                    }"""

    # 反序列化 JSON 字符串为字典
    data = json.loads(json_str)

    # 创建 JobSettings 对象
    job_settings = JobSettingsDict(json_str)
    # a = job_settings.as_dict()
    # #a = job_settings.recursive_simplenamespace(a)
    # for cluster in a.job_clusters:
    #     print(cluster.new_cluster)
    #job_settings = job_settings.recursive_simplenamespace(job_settings)
    for cluster in job_settings.job_clusters:
        cluster["new_cluster"]["num_workers"] = 2
    # setattr(a, 'as_dict', lambda : a)
    # setattr(a, '__dict__', lambda : a)

    job_clusters = {"bp_job":[{
                        "job_cluster_key": "Job_cluster002",
                        "new_cluster": {
                            "num_workers": 1,
                            "spark_version": "14.3.x-scala2.12",
                            "spark_conf": {},
                            "azure_attributes": {
                                "first_on_demand": 1,
                                "availability": "ON_DEMAND_AZURE",
                                "spot_bid_max_price": -1
                            },
                            "node_type_id": "Standard_D4ds_v5",
                            "ssh_public_keys": [],
                            "custom_tags": {},
                            "spark_env_vars": {},
                            "init_scripts": [],
                            "data_security_mode": "USER_ISOLATION",
                            "runtime_engine": "PHOTON"
                        }
                        }
                   ]}
    task_clusters = { "load_bp": "Job_cluster001" }

    j = {
            "job_clusters": [
                {
                    "job_cluster_key": "Job_cluster002",
                    "new_cluster": {
                        "num_workers": 1,
                        "spark_version": "14.3.x-scala2.12",
                        "spark_conf": {},
                        "azure_attributes": {
                            "first_on_demand": 1,
                            "availability": "ON_DEMAND_AZURE",
                            "spot_bid_max_price": -1
                        },
                        "node_type_id": "Standard_D4ds_v5",
                        "ssh_public_keys": [],
                        "custom_tags": {},
                        "spark_env_vars": {},
                        "init_scripts": [],
                        "data_security_mode": "USER_ISOLATION",
                        "runtime_engine": "PHOTON"
                    }
                }
            ],
            "task_cluster": {
                "load_bp": "Job_cluster001"
            }
        }

    for job_name, job_cluster_defs in job_clusters.items():
        for base_job in workspace.jobs.list(name=job_name):
            job = workspace.jobs.get(base_job.job_id)
            
            for job_cluster_def in job_cluster_defs:
                if job.settings.job_clusters is not None:
                    clusters = [job_cluster for job_cluster in job.settings.job_clusters if job_cluster.job_cluster_key == job_cluster_def["job_cluster_key"]]
                    for cluster in clusters:
                        job.settings.job_clusters.remove(cluster)
                else:
                    job.settings.job_clusters = []
                job_cluster_key = job_cluster_def["job_cluster_key"]
                job.settings.job_clusters.append(JobCluster(job_cluster_key).from_dict(job_cluster_def))
            workspace.jobs.reset(base_job.job_id,job.settings)

    print(job.settings.job_clusters)
    # for task_name, cluster in task_clusters.items():
    #     for task in [task for task in workspace.jobs.list(expand_tasks=True) if task.task_key == task_name]:
            
    #         task.job_cluster_key = 
    
    #print(job_cluster)
        
    #print(job.settings.name)
    # for task in job.settings.tasks:
    #     pass
        #print(task.existing_cluster_id, task, sep='\n')

