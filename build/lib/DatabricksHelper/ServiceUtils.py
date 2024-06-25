from DatabricksHelper.Service import PipelineService
from pyspark.sql import DataFrame, SparkSession 
import hashlib

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
        
        user = self.pipeline_service.spark_session.sql('select current_user()').collect()[0][0]

        if stage_table:
            if catalog:
                table_name = f"`{catalog}`.`{schema}`.`{view_name}_{user}`"
            else:
                table_name = f"`{schema}`.`{view_name}_{user}`"

            self.pipeline_service.spark_session.sql(f"drop table if exists {table_name}").collect()
            self.pipeline_service.spark_session.sql(f"create table {table_name} as {query}").collect()
            cache_dataframe = self.pipeline_service.spark_session.sql(f"select * from {table_name}")
            cache_dataframe.cache()
        else:
            cache_dataframe = self.pipeline_service.spark_session.sql(query)
            cache_dataframe.cache()
        if view_name:
            cache_dataframe.createOrReplaceTempView(view_name)
        return cache_dataframe
    
    def parse_task_params(self, task_params):
        return self.pipeline_service.parse_task_params(task_params)

    def sql_params(self, params):
        for key, value in params.items():
            self.sql_param(key, value)

    def sql_param(self, key, value):
        self.pipeline_service.databricks_dbutils.widgets.text(key, value)

    def __init__(self, spark:SparkSession=None) -> None:
        self.pipeline_service = PipelineService(spark)