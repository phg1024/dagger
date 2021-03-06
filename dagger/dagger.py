from typing import List
import functools
import time
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from .utils import timed


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


class TaskNode(object):
    def __init__(self, name, task, inputs, deps):
        self.name = name
        self.task = task
        self.deps = deps
        self.unfinished_deps = len(deps)
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
        return f'{self.name}, {str(self.task)}, {self.inputs}'


class TaskDAG(object):
    def __init__(self, verbose=False):
        self.tasks = {}
        self.verbose = verbose
        self.pending_tasks = set()
        self.current_tasks = set()
        self.dag = {}
        self.launch_queue = []

    def add(self, name: str, task: Task, deps: List[str] = [], inputs=([], {})):
        assert isinstance(task, Task), 'You can only add Task instance to the DAG'
        assert name not in self.tasks
        if self.verbose:
            print('Adding', task)
        self.tasks[name] = TaskNode(name, task, inputs, deps)
        if self.verbose:
            print('All tasks:', self.tasks)

    def _build_dag(self):
        # dag is actually just an adjacency list
        self.dag = {name: set() for name in self.tasks.keys()}

        for name, task in self.tasks.items():
            for dep in task.deps:
                self.dag[dep].add(name)

    def _launch_tasks(self, pool, verbose=False):
        ready_tasks = []
        finished_tasks = []
        # Process the launch queue
        if verbose:
            print('Current launch queue:', self.launch_queue)
        for task in self.launch_queue:
            if verbose:
                print(f'Checking {task.name} ...')
            if task.done():
                if verbose:
                    print(task.name, 'finished @', time.time())
                task.stop()

                # check if one of its children task can be added to the launch stack
                for child in self.dag[task.name]:
                    child_task = self.tasks[child]
                    child_task.unfinished_deps -= 1
                    if child_task.unfinished_deps == 0:
                        ready_tasks.append(child_task)

                finished_tasks.append(task.name)
                continue
            elif verbose:
                print(f'{task.name} is not done.')

            can_start = not task.started()

            # gather the status from dependent tasks
            dres = []
            for d in task.deps:
                if not self.tasks[d].finished():
                    can_start = False
                else:
                    dres.append(self.tasks[d].result)

            if can_start:
                if verbose:
                    print('Starting', task.name, '@', time.time())
                task.start(pool, dres)

        self.launch_queue = [x for x in self.launch_queue if x.name not in finished_tasks]
        self.launch_queue.extend(ready_tasks)

        return len(self.launch_queue) > 0

    def execute(self, verbose=None):
        if verbose is None:
            verbose = self.verbose

        with timed('DAG execution', verbose=verbose):
            pool = ThreadPoolExecutor(3)

            self._build_dag()

            # The initial launch stack
            self.launch_queue = [task for _, task in self.tasks.items() if task.unfinished_deps == 0]

            while True:
                if not self._launch_tasks(pool, verbose):
                    break

                time.sleep(0.005)
            if verbose:
                print('done')

        if verbose:
            total_execution_time = 0
            for name, task in self.tasks.items():
                print(f'[task] {task.task.func.__name__,} {name}')
                print(f'output: {task.result}')
                print(f"start: {task.t_start}")
                print(f"end: {task.t_end}")
                print(f"elapsed: {task.elapsed}")
                total_execution_time += task.elapsed
            print(f'CPU wall time: {total_execution_time} seconds')
