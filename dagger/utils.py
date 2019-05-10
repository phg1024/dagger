import contextlib
import time


@contextlib.contextmanager
def timed(item, verbose=True):
    t0 = time.time()
    yield
    t1 = time.time()
    if verbose:
        print(f'Time cost for {time} is {t1 - t0} seconds')
