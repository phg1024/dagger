# dagger
A poor man's DAG based task scheduler

## Example
Any regular python function can be turned into a task with the `Task` decorator:
```
@Task
def do_something(*args, **kwargs):
  ...

@Task
def do_someting_else(*args, **kwargs):
  ...
```

To construct a task DAG, simply add tasks to it:
```
dag = TaskDAG()
dag.add('first task', do_something, inputs=(['buy milk'], {}))
dag.add('second task', do_something, inputs=(['do laundry', 'cook meal'], {}))
dag.add('last task', do_someting_else, deps=['first task', 'second task'])
```

After constructing the task DAG, executing it is just one line of code:
```
dag.execute()
```


