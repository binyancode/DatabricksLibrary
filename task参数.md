# Job级别参数：
## **default_catalog**
* 默认catalog，字符串，必填项
## **job_name**
* 要运行的job name，字符串，必填项，创建databricks job的时候，要求job name唯一
## **continue_run**
* 是否持续运行，布尔类型，必填项，建议默认值true
## **reload_table**
* reload策略，枚举类型，必填项，建议默认值：Reload.DEFAULT
* 可选：Reload.DEFAULT | Reload.CLEAR_SCHEMA | Reload.CLEAR_CHECKPOINT | Reload.DROP_TABLE | Reload.TRUNCATE_TABLE

# Task级别参数
格式为 {"task_key": 值}，其中task_key为job的task名称
## **source_file**
* 数据源的路径，对象类型
* 参考值：{"task_key":{ "value"="路径" } }
## **reader_options**
* 数据加载选项，对象类型
* 参考值：{"task_key":{"cloudFiles.maxFileAge":"1 year", "cloudFiles.partitionColumns":""}}
## **table_alias**
* 加载的数据的表别名
* 参考值：{"task_key":"businesspartner"}
## **target_table**
* 存放临时数据的表名
* 参考值：{"task_key":"temp.businesspartner"}，通常建议存放于temp的schema下
## **max_load_rows**
* 一次性加载数据的最大行数
* 参考值：{"task_key":50000}
file_format，数据源格式
参考值{"task_key":"json"}，支持parquet，csv，json
## **notebook_path**
* 动态transform的notebook
* 参考值：{"task_key":"/Workspace/eVA/transform/Transform_BusinessPartner"}
* 注意task_key所指的task必须是执行/Workspace/eVA/workflow_core/run_notebook的task，否则传递无效
## **notebook_timeout**
* 动态transform的notebook执行超时秒数
* 参考值：{"task_key":3600}
## **task_parameters**
* transform的动态参数
* 参考值： {"task_key":{"key1":"value1", "key2":"value2"}}

> 以上参数也可以直接在job的task里赋值，如果都有赋值，这里的赋值优先级高