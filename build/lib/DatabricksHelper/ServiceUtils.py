from DatabricksHelper.Service import PipelineService
from pyspark.sql import DataFrame, SparkSession
from types import SimpleNamespace
import hashlib
import json

class PipelineUtils:
    def __string_to_md5(self, input_string):
        md5_obj = hashlib.md5()
        md5_obj.update(input_string.encode('utf-8'))
        md5_value = md5_obj.hexdigest()
        return md5_value
    
    def change_table_owner(self, table_name, new_owner = None):
        if not new_owner:
            new_owner = self.pipeline_service.spark_session.sql('select current_user()').collect()[0][0]
        sql = f'alter table {table_name} set owner to `{new_owner}`'
        print(sql)
        self.pipeline_service.spark_session.sql(sql).collect()

    def check_table_exists(self, catalog, schema, table_name):
        if catalog and schema:
            scope = f"`{catalog}`.`{schema}`"
        elif schema:
            scope = f"`{schema}`"
        else:
            scope = None
        if scope:
            return self.pipeline_service.spark_session.sql(f"show tables in {scope} like '{table_name}'").count() > 0
        else:
            return self.pipeline_service.spark_session.sql(f"show tables like '{table_name}'").count() > 0

    def cache(self, data, view_name:str = None, stage_table = False, catalog = None, schema = "cache"):
        query = None
        if isinstance(data, str):
            query = data
        elif isinstance(data, DataFrame):
            data.createOrReplaceTempView(f"{view_name}_dataframe")
            query = f"select * from {view_name}_dataframe"
        else:
            raise Exception("Invalid data type")
        
        user = self.pipeline_service.spark_session.sql('select current_user()').collect()[0][0].replace('@', '#').replace('.', '#')

        if stage_table:
            if catalog:
                table_name = f"`{catalog}`.`{schema}`.`{view_name}_{user}`"
            else:
                table_name = f"`{schema}`.`{view_name}_{user}`"
            print(table_name)
            self.pipeline_service.spark_session.sql(f"drop table if exists {table_name}").collect()
            self.pipeline_service.spark_session.sql(f"create table {table_name} as {query}").collect()
            cache_dataframe = self.pipeline_service.spark_session.table(f"{table_name}")
            cache_dataframe.cache()
        else:
            cache_dataframe = self.pipeline_service.spark_session.sql(query)
            cache_dataframe.cache()
        if view_name:
            cache_dataframe.createOrReplaceTempView(view_name)
        return cache_dataframe

    def __init_params(self, parameter_list):
        params = {}
        for parameter in parameter_list:
            name = parameter[0] if isinstance(parameter, tuple) else parameter
            default_value = parameter[1] if isinstance(parameter, tuple) and len(parameter) > 1 else ""
            eval(f'self.pipeline_service.databricks_dbutils.widgets.text("{name}", "{default_value}")')
            exec(f'params["{name}"] = self.pipeline_service.databricks_dbutils.widgets.get("{name}")')
            if isinstance(parameter, tuple) and len(parameter) > 2:
                exec(f'params["{name}"] = {parameter[2]}(params["{name}"]) if params["{name}"] and isinstance(params["{name}"], str) else ""')
        return params
    
    
    def init_run_params(self):
        params = self.init_common_params(["job_name", "target_table", "source_file", "file_format", "table_alias", \
                                          "reader_options", "reload_table", "max_load_rows", ("continue_run", "True", "bool"), \
                                            ("timeout", "3600", "int"), "notebook_path", ("notebook_timeout", "-1", "int")], False)
        # parameter_list = ["pipeline_run_id", "pipeline_name", "job_name", "default_catalog", "target_table", \
        #                   "source_file", "file_format", "table_alias", "reader_options", "reload_table", \
        #                     "max_load_rows", ("continue_run", "True", "bool"), ("timeout", "3600", "int"), 
        #                     "notebook_path", ("notebook_timeout", "-1", "int"), "task_parameters"]
        # params = self.__init_params(parameter_list)
        
        params = vars(params)
        params["job_params"] = {}
        for key, value in params.items():
            if key != "job_params" and value is not None and value != "" and (not isinstance(value, str) or value.strip("{}").strip(" ") != ""):
                params["job_params"][key] = str(value) if isinstance(value, (int, float, str, bool)) else json.dumps(value)
        params = SimpleNamespace(**params)
        return params

    def init_load_params(self):
        params = self.init_common_params(["target_table","source_file", "file_format", "table_alias", \
                                          ("reader_options","{}","json.loads"), ("reload_table", "Reload.DEFAULT"), \
                                            ("max_load_rows", "-1", "int"), "validation"], False)
        # parameter_list = ["pipeline_run_id", "pipeline_name", "default_catalog", "target_table", \
        #                   "source_file", "file_format", "table_alias", ("reader_options","{}","json.loads"), \
        #                     ("reload_table", "Reload.DEFAULT"), ("max_load_rows", "-1", "int"), "validation", "task_parameters"]
        # params = self.__init_params(parameter_list)
        # params = SimpleNamespace(**params)
        return params
    
    def init_run_notebook_params(self):
        params = self.init_common_params(["notebook_path", ("notebook_timeout", "-1", "int"), "task_load_info"], False)
        #parameter_list = ["pipeline_run_id", "pipeline_name", "default_catalog", "notebook_path", ("notebook_timeout", "-1", "int"), "task_load_info", "task_parameters"]
        #params = self.__init_params(parameter_list)
        #params = SimpleNamespace(**params)
        return params

    def init_transform_params(self):
        params = self.init_common_params(["task_load_info"])
        params = vars(params)
        if params["task_load_info"] and isinstance(params["task_load_info"], str):
            try:
                params["task_load_info"] = json.loads(params["task_load_info"])
            except Exception as e: 
                print(e)
        params = SimpleNamespace(**params)
        return params

    def init_common_params(self, parameter_list = None, parse_task_param = True):
        if parameter_list:
            parameter_list = parameter_list + ["pipeline_run_id", "pipeline_name", "default_catalog", "task_parameters"]
        else:
            parameter_list = ["pipeline_run_id", "pipeline_name", "default_catalog", "task_parameters"]
        params = self.__init_params(parameter_list)
        if parse_task_param:
            params["task_parameters"] = self.parse_task_param(params["task_parameters"])
            if params["task_parameters"] and isinstance(params["task_parameters"], str):
                try:
                    params["task_parameters"] = json.loads(params["task_parameters"])
                except Exception as e: 
                    print(e)
        params = SimpleNamespace(**params)
        return params

    def parse_task_param(self, task_params):
        return self.pipeline_service.parse_task_param(task_params)

    def get_task_values(self):
        return self.pipeline_service.get_task_values()

    def get_notebook(self, notebook_path):
        return self.pipeline_service.get_notebook(notebook_path)
    
    def sql_params(self, params):
        for key, value in params.items():
            self.sql_param(key, value)

    def sql_param(self, key, value):
        self.pipeline_service.databricks_dbutils.widgets.text(key, value)

    def __init__(self, spark:SparkSession=None) -> None:
        self.pipeline_service = PipelineService(spark)