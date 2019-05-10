from typing import List
import functools
import time
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
import contextlib

class Task(object):
  def __init__(self, func):
    functools.update_wrapper(self, func)
    self.func = func
  
  def __call__(self, *args, **kwargs):
    self.results = self.func(*args, **kwargs)
    return self.results

  def __repr__(self):
    return f'{self.func.__name__}'

class Status(Enum):
  NotStarted = 0
  Running = 1
  Finished = 2
 
@contextlib.contextmanager
def timed(item):
  t0 = time.time()
  yield
  t1 = time.time()
  print('Time cost for {} is {} seconds'.format(item, t1 - t0))

class TaskNode(object):
  def __init__(self, name, task, inputs, deps):
    self.name = name
    self.task = task
    self.deps = deps
    self.inputs = inputs
    self.status = Status.NotStarted
    self.t_start = None
    self.t_end = None
    self.fut = None
  
  def start(self, pool, dres):
    self.status = Status.Running
    self.t_start = time.time()
    args, kwargs = self.inputs
    self.fut = pool.submit(self.task, *(args + dres), **kwargs)
    return self.fut

  def stop(self):
    self.status = Status.Finished
    self.t_end = time.time()
    self.elapsed = self.t_end - self.t_start
    self.result = self.fut.result()
  
  def started(self):
    return self.status != Status.NotStarted

  def done(self):
    if self.fut is None:
      return False
    return self.fut.done()
  
  def finished(self):
    return self.status == Status.Finished

  def __repr__(self):
    return f'{str(self.task)}, {self.inputs}'

class TaskDAG(object):
  def __init__(self):
    self.tasks = {}
  
  def add(self, name: str, task: Task, deps: List[str] = [], inputs=([], {})):
    assert isinstance(task, Task), 'You can only add Task instance to the DAG'
    assert name not in self.tasks
    print('Adding', task)
    self.tasks[name] = TaskNode(name, task, inputs, deps)

    print('All tasks:', self.tasks)

  def execute(self):
    with timed('DAG execution'):
      pool = ThreadPoolExecutor(3)
      
      while True:
        for k, task in self.tasks.items():
          if task.finished():
            continue

          can_start = not task.started()

          # gather the status from dependent tasks
          dres = []
          for d in task.deps:
            if not self.tasks[d].finished():
              can_start = False
            else:
              dres.append(self.tasks[d].result)
          
          if can_start:
            print('Starting', k, '@', time.time())
            task.start(pool, dres)
        
        # check all the running tasks and update status if needed
        all_done = True
        for k, task in self.tasks.items():
          if task.finished():
            continue

          if task.done():
            print(k, 'finished @', time.time())
            task.stop()
          
          if not task.finished():
            all_done = False

        if all_done:
          break

        time.sleep(0.005)
    
      print('done')

    total_execution_time = 0
    for name, task in self.tasks.items():
      print('[task] {} {}'.format(task.task.func.__name__, name))
      print('output: {}'.format(task.result))
      print("start: {}".format(task.t_start))
      print("end: {}".format(task.t_end))
      print("elapsed: {}".format(task.elapsed))
      total_execution_time += task.elapsed
    print(f'CPU wall time: {total_execution_time} seconds')
