import contextlib
import time

@contextlib.contextmanager
def timed(item, verbose=True):
  t0 = time.time()
  yield
  t1 = time.time()
  if verbose:
    print('Time cost for {} is {} seconds'.format(item, t1 - t0))
