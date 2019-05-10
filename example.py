from dagger.dagger import Task, TaskDAG, timed
import time

@Task
def f(n):
  time.sleep(0.5)
  total = 0
  for i in range(n):
    total += n * n
  return total

@Task
def g(*args):
  time.sleep(0.5)
  return sum(args)

@Task
def h(a, b):
  return a / b    

if __name__ == '__main__':
  with timed('DAG execution') as t:
    dag = TaskDAG()
    dag.add('f1', f, inputs=([100], {}))
    dag.add('f2', f, inputs=([200], {}))
    dag.add('f3', f, inputs=([200], {}))
    dag.add('g', g, deps=['h', 'f2', 'f3'])
    dag.add('h', h, deps=['f1', 'f2'])
    dag.execute(verbose=True)

  with timed("sequential execution") as t:
    r1 = f(100)
    r2 = f(200)
    r3 = f(50)
    r4 = h(r1, r2)
    g(r4, r2, r3)
