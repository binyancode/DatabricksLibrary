from pyspark.sql import SparkSession, Row, Column
from pyspark.sql.functions import udf, struct, from_json
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, BooleanType
import json
from typing import Tuple
import inspect

from abc import ABC, abstractmethod
class TableLoadingValidation(ABC):
    @abstractmethod
    def validate(self, row) -> Tuple[bool, str]:
        pass

class Test:
    def run(self):
        script = """
class BPValidation(TableLoadingValidation):
    def validate(self, row) -> Tuple[bool, str]:
        return {"name":"validation1","is_valid":True, "error_message":row.json_struct.name + " is valid"}
"""

        globals_dict = globals()
        globals_dict['TableLoadingValidation'] = TableLoadingValidation
        exec(script, globals_dict)
        #exec(script) 

        def find_subclasses_of_a():
            subclasses = []
            for name, obj in globals().items():
                
                if isinstance(obj, type):  # 确保是一个类
                    #print(name, TableLoadingValidation in BPValidation.__mro__,obj.__bases__, TableLoadingValidation.__name__)
                    if issubclass(obj, TableLoadingValidation) and obj is not TableLoadingValidation:  # 检查是否是A的子类，排除A本身
                        subclasses.append(obj())
            return subclasses
        
        subclasses = find_subclasses_of_a()
        for subclass in subclasses:
            print(subclass)
        
        module = __import__(self.__module__)
        def find_subclasses_of_b():
            subclasses = []
            for name, obj in inspect.getmembers(module):
                #print(name)
                if inspect.isclass(obj) and issubclass(obj, TableLoadingValidation) and obj is not TableLoadingValidation:
                    subclasses.append(obj)
            return subclasses

        # 使用函数并打印结果
        subclasses_of_b = find_subclasses_of_b()
        for subclass in subclasses_of_b:
            print(subclass.__name__)


        validation = BPValidation()

        schema = StructType([
            StructField("result", ArrayType(
                StructType([
                    StructField("name", StringType(), True),
                    StructField("is_valid", BooleanType(), True),
                    StructField("error_message", StringType(), True)
                ])
            ), True)
        ])

        schema = ArrayType(
                StructType([
                    StructField("name", StringType(), True),
                    StructField("is_valid", BooleanType(), True),
                    StructField("error_message", StringType(), True)
                ])
            )

        data = [("1", '{"name": "John", "age": 30}'), ("2", '{"name": "Jane", "name2":"Jane2", "age": 25}')]
        json_schema = StructType([
                StructField("name", StringType(), True),
                StructField("name2", StringType(), True),  # 确保所有字段都遵循 StructField 定义
                StructField("age", IntegerType(), True)
            ])
        df = spark.createDataFrame(data = data, schema=StructType([
                StructField("id", StringType(), True),
                StructField("json_string", StringType(), True)
            ]))

        df = df.withColumn("json_struct", from_json(df["json_string"], json_schema))
        @udf(schema)
        def process_data(row):
            return [validation.validate(row)]
            #return ("name", {"is_valid": False, "error_message": "Invalid name"})

        df = df.withColumn("valid_result", process_data(struct([df[col] for col in df.columns])))
        display(df)


from pyspark.sql.functions import col, filter, size
result_schema = ArrayType(
    StructType([
        StructField("name", StringType(), True),
        StructField("is_valid", BooleanType(), True),
        StructField("error_message", StringType(), True)
    ])
)

# Sample data
data = [
    ("1", "John Doe", [{"name": "load_id_validation", "is_valid": True, "error_message": "1 is valid"},
                       {"name": "load_id_validation", "is_valid": False, "error_message": "1 is not valid"}]),
    ("2", "Jane Doe", [{"name": "load_id_validation", "is_valid": True, "error_message": "error message for Jane Doe"}])
]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "name", "result"])

filtered_df = df.filter(size(filter(col("result"), lambda x: x["is_valid"] == False)) == 0)

# Show the filtered DataFrame
filtered_df.show()

%sql
SELECT *
FROM t
WHERE EXISTS (
    SELECT 1
    FROM explode(t.validations.results) AS v
    WHERE v.a = 1
)

# schema = StructType([
#     StructField("is_valid", BooleanType(), True),
#     StructField("error_message", StringType(), True)
# ])
#validation = BPValidation()
Test().run()


 ###############
import types

class b:
    pass
class c:
    def run(self):
        # 定义要执行的代码
        code_to_exec = """
import json
class MyClass(b):
    def func(row):
        print(json.loads(row))
        MyClass.my_function()

    def my_function():
        print("Hello from my_function!")
class MyClass2:
    def func(self, row):
        print(json.loads(row))
        self.my_function()

    def my_function(self):
        print("Hello from my_function!")
        """

        # 获取执行前的全局作用域快照
        locals_before = dict(locals())

        # 执行代码
        exec(code_to_exec)

        # 获取执行后的全局作用域快照
        locals_after = dict(locals())

        # 找出新增的条目
        new_locals = {k: v for k, v in locals_after.items() if k not in locals_before}

        # 过滤出类和函数
        new_classes_and_functions = {name: obj for name, obj in new_locals.items() if isinstance(obj, (type, types.FunctionType))}

        # 打印结果
        for key, value in new_classes_and_functions.items():
            print("Newly defined classes and functions:", value(), issubclass(value, b))
c().run()


##############


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