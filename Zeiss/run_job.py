# Databricks notebook source
from DatabricksHelper.Service import Pipeline, Reload
from DatabricksHelper.ServiceUtils import PipelineUtils
from types import SimpleNamespace
import uuid

# COMMAND ----------

p_u = PipelineUtils()
params = p_u.init_run_params()
print(params)

# COMMAND ----------

p = Pipeline(params.pipeline_run_id, params.default_catalog, params.pipeline_name)
run = p.run(params.job_name, params.job_params, params.continue_run)

