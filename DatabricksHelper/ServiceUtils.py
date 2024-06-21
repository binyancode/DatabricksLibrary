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
    
    def __init__(self, spark:SparkSession=None) -> None:
        self.pipeline_service = PipelineService(spark)