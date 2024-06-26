from pyspark.sql import SparkSession, Row, Column
from pyspark.sql.functions import udf, struct, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
import json

schema = StructType([
    StructField("is_valid", BooleanType(), True),
    StructField("valid_result", IntegerType(), True)
])

@udf(schema)
def process_data(row):

    json.loads('{}')
    return (True, 123)



# 示例数据，其中一列包含 JSON 字符串
data = [("1", '{"name": "John", "age": 30}'), ("2", '{"name": "Jane", "name2":"Jane2", "age": 25}')]

# 创建 DataFrame
df = spark.createDataFrame(data, ["id", "json_string"])

# 定义 JSON 字符串的 schema
json_schema = StructType([
    StructField("name", StringType(), True),
    StructField("name2", StringType()),
    StructField("age", IntegerType(), True)
])

# 使用 from_json 函数将 json_string 列转换为 struct 类型
df = df.withColumn("json_struct", from_json(df["json_string"], json_schema))

df = df.withColumn("_is_valid", process_data(struct([df[col] for col in df.columns])))
# 显示 DataFrame
display(df)


from abc import ABC, abstractmethod

# 定义抽象基类
class Parent(ABC):
    @abstractmethod
    def my_method(self):
        pass

# 定义子类，继承自 Parent
class Child(Parent):
    # 实现父类中的抽象方法
    def my_method(self):
        print("子类实现了父类的抽象方法")

# 创建子类实例
child_instance = Child()

# 调用实现的方法
child_instance.my_method()