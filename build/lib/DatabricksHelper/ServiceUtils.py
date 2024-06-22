from DatabricksHelper.Service import PipelineService
from pyspark.sql import DataFrame, SparkSession 

class PipelineUtils:
    def cache(self, data, view_name:str = None):
        if isinstance(data, str):
            data = self.pipeline_service.spark_session.sql(data)
        elif isinstance(data, DataFrame):
            data = data
        data.cache()
        if view_name:
            data.createOrReplaceTempView(view_name)
        return data
    
    def parse_task_params(self, task_params):
        return self.pipeline_service.parse_task_params(task_params)

    def sql_params(self, params):
        for key, value in params.items():
            self.sql_param(key, value)

    def sql_param(self, key, value):
        self.pipeline_service.databricks_dbutils.widgets.text(key, value)

    def __init__(self, spark:SparkSession=None) -> None:
        self.pipeline_service = PipelineService(spark)