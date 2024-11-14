# Databricks notebook source
from DatabricksHelper.ServiceUtils import PipelineUtils
import json

# COMMAND ----------

p_u = PipelineUtils()
params = p_u.init_run_load_notebook_params()
print(params)

# COMMAND ----------

params.notebook_path = p_u.parse_task_param(params.notebook_path)
params.notebook_timeout = p_u.parse_task_param(params.notebook_timeout)
params.task_load_info = json.dumps(p_u.get_task_values())
params.reload_info = json.dumps(params.reload_info)
print(params)

# COMMAND ----------


dbutils.notebook.run(params.notebook_path, params.notebook_timeout, arguments=vars(params))

