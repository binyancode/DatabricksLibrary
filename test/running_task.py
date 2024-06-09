from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState, RunState, ListRunsRunType, JobCluster, JobSettings, JobTaskSettings, NotebookTask,NotebookTaskSource
import json
workspace = WorkspaceClient(host="https://adb-732672050507723.3.databricks.azure.cn/", token="dapi91bc55dd4e482c68198ebb264a9b775a")
for cluster in workspace.clusters.list():
    pass
from collections import defaultdict, deque


def task_levels(tasks):
    # 创建一个字典来存储每个任务的依赖任务
    dependencies = defaultdict(list)
    for task in tasks:
        for dep in task.get('depends_on', []):
            dependencies[dep['task_key']].append(task['task_key'])

    # 创建一个队列来存储需要处理的任务和它们所在的层级
    queue = deque([(task['task_key'], 0) for task in tasks if not task.get('depends_on')])

    # 创建一个字典来存储每个层级的任务数量
    level_counts = defaultdict(int)

    while queue:
        task, level = queue.popleft()
        level_counts[level] += 1
        for next_task in dependencies[task]:
            queue.append((next_task, level + 1))

    return level_counts
    
def max_parallel(tasks):
    task_dict = {task['task_key']: task for task in tasks}
    root_tasks = [task for task in tasks if not task.get('depends_on')]
    Q = root_tasks
    max_width = 0

    while Q:
        n = len(Q)
        max_width = max(max_width, n)
        for _ in range(n):
            task = Q.pop(0)
            for dependent_task in tasks:
                if dependent_task.get('depends_on') is not None:
                    if any(dependency['task_key'] == task['task_key'] for dependency in dependent_task.get('depends_on')):
                        Q.append(dependent_task)

    return max_width

for base_run in workspace.jobs.list_runs(run_type=ListRunsRunType.JOB_RUN):
    if base_run.state.life_cycle_state not in [RunLifeCycleState.TERMINATED, RunLifeCycleState.INTERNAL_ERROR, RunLifeCycleState.SKIPPED, RunLifeCycleState.TERMINATING]:
        print(base_run.state.life_cycle_state)
        run = workspace.jobs.get_run(base_run.run_id)
        #print(json.dumps(run.as_dict()))
        tasks = [task for task in run.tasks if task.state.life_cycle_state not in [RunLifeCycleState.TERMINATED, RunLifeCycleState.INTERNAL_ERROR, RunLifeCycleState.SKIPPED, RunLifeCycleState.TERMINATING]]
        for task in tasks:
            print(task.task_key, task.state.life_cycle_state, task.depends_on)
        print(max_parallel([task.as_dict() for task in tasks]))

        levels = task_levels([task.as_dict() for task in tasks if task.state.life_cycle_state not in [RunLifeCycleState.TERMINATED, RunLifeCycleState.INTERNAL_ERROR, RunLifeCycleState.SKIPPED, RunLifeCycleState.TERMINATING]])
        for level, count in levels.items():
            print(level, count)



# tasks = [
#     {'task_key': 'task0', 'state': {'life_cycle_state': 'RUNNING'}, 'depends_on': None},
#     {'task_key': 'task1', 'state': {'life_cycle_state': 'RUNNING'}, 'depends_on': None},
#     {'task_key': 'task2', 'state': {'life_cycle_state': 'RUNNING'}, 'depends_on': [{'task_key': 'task1'}]},
#     {'task_key': 'task3', 'state': {'life_cycle_state': 'RUNNING'}, 'depends_on': [{'task_key': 'task1'}]},
#     {'task_key': 'task4', 'state': {'life_cycle_state': 'RUNNING'}, 'depends_on': [{'task_key': 'task2'}, {'task_key': 'task3'}]}
# ]



# print(max_width(tasks))