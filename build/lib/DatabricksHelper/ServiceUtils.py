from DatabricksHelper.Service import PipelineService
from pyspark.sql import DataFrame, SparkSession 

class PipelineUtils:
    def cache(self, data, view_name:str = None, stage_table = False, catalog = None, schema = "cache"):
        query = None
        if isinstance(data, str):
            query = data
        elif isinstance(data, DataFrame):
            data.createOrReplaceTempView(f"{view_name}_dataframe")
            query = f"select * from {view_name}_dataframe"
        else:
            raise Exception("Invalid data type")
        if stage_table:
            if catalog:
                table_name = f"{catalog}.{schema}.{view_name}"
            else:
                table_name = f"{schema}.{view_name}"
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