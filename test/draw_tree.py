from asciitree import LeftAligned
from collections import defaultdict

def draw_tree(tasks):
    # 创建一个字典，将每个任务的键映射到它的依赖项
    task_dict = defaultdict(list)
    for task in tasks:
        if task['depends_on']:
            for dep in task['depends_on']:
                task_dict[dep['task_key']].append(task['task_key'])

    # 创建一个嵌套的字典结构
    def nested_dict(task_key):
        return {task_key: nested_dict(dep) for dep in task_dict[task_key]}
    print(task_dict)
    # 生成ASCII树
    tree = nested_dict('task1')  # 假设 'task0' 是根任务
    tr = LeftAligned()
    print(tr(tree))

tasks = [
    {'task_key': 'task0', 'depends_on': None},
    {'task_key': 'task1', 'depends_on': None},
    {'task_key': 'task2', 'depends_on': [{'task_key': 'task1'}]},
    {'task_key': 'task2.1', 'depends_on': [{'task_key': 'task2'}]},
    {'task_key': 'task3', 'depends_on': [{'task_key': 'task1'}]},
    {'task_key': 'task4', 'depends_on': [{'task_key': 'task2'}, {'task_key': 'task3'}]},
    {'task_key': 'task4.1', 'depends_on': [{'task_key': 'task4'}]},
    {'task_key': 'task5', 'depends_on': [{'task_key': 'task0'}]},
    {'task_key': 'task6', 'depends_on': [{'task_key': 'task5'}]},
    {'task_key': 'task7', 'depends_on': [{'task_key': 'task5'}]},
]

draw_tree(tasks)