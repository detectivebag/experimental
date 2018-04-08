"""
There are many ways to do parallel in pandas
e.g.
1 https://gist.github.com/tejaslodaya/562a8f71dc62264a04572770375f4bba
2 http://www.racketracer.com/2016/07/06/pandas-in-parallel/
3 http://pythonhosted.org/rosetta/#module-rosetta.parallel.parallel_easy
4 google dask pandas

For groupby methods:
1    # return pd.concat(list(filter(lambda e: type(e) is str, ret_list)))
    # # retLst = Parallel(n_jobs=multiprocessing.cpu_count())(delayed(func)(group) for name, group in dfGrouped)
    # return pd.concat([pd.DataFrame(i) for i in ret_list])
2   http://josepferrandiz.blogspot.com/2014/12/python-pandas-and-multiprocessing.html
3   https://blakeboswell.github.io/2016/parallel-groupby/
4   https://stackoverflow.com/questions/26187759/parallelize-apply-after-pandas-groupby
5   http://star.mit.edu/cluster/
6   http://dask.pydata.org/en/latest/dataframe-groupby.html#aggregate
7   http://pythonhosted.org/rosetta/_modules/rosetta/parallel/pandas_easy.html
"""

import itertools
import multiprocessing
import multiprocessing as mp
import time
from functools import partial, wraps
from typing import Callable, Tuple, Union

import numpy as np
import pandas as pd

from explorex.utils.dataframe_util import flatten_any, flatten_array, normalize_json, implicit_to_counter
from explorex.utils.basic_util import logger, deprecated

operator_map = {
    'normalize_json': normalize_json,
    'flatten_any': flatten_any,
    'flatten_array': flatten_array
}


def np_splits(df):
    return np.array_split(df, mp.cpu_count())


def fix_size_splits(df, chunk_size=5000):
    list_of_df = list()
    number_chunks = len(df) // chunk_size + 1
    for i in range(number_chunks):
        list_of_df.append(df[i * chunk_size:(i + 1) * chunk_size])
    return list_of_df


def identity_map(df):
    return [df]


split_strategy_map = {
    'np': np_splits,
    'size': fix_size_splits,
    'id': identity_map
}


def split_strategy(data, chunk_size=15000):
    if len(data) < 64:
        print("MapReduce strategy: {} \n"
              "Dataset size      : {} \n".format('identity', len(data)))
        return identity_map
    if len(data) > mp.cpu_count() * chunk_size:
        print("MapReduce strategy: {} \n"
              "Dataset size      : {} \n".format('chunk', len(data)))
        return partial(fix_size_splits, chunk_size=chunk_size)
    else:
        print("MapReduce strategy: {} \n"
              "Dataset size      : {} \n".format('np', len(data)))
        return np_splits


# TODO: the order of this the dataframe may be important, this splits may not keep this is correctly ordered.
# https://gist.github.com/tejaslodaya/562a8f71dc62264a04572770375f4bba this link may be helpful
@logger
def parallel_runner(fn_type, *args, **kwargs):
    data = args[0]

    data_chunks = split_strategy(data)(data)

    with mp.Pool() as pool:
        results = pool.starmap(partial(operator_map.get(fn_type, lambda e: e), **kwargs),
                               ((i,) + args[1:] for i in data_chunks))
    return pd.concat(results).reset_index(drop=True)


@deprecated("this decorator is not working:"
            "more details can be found in this link"
            "https://stackoverflow.com/questions/11731664/python-making-a-class-method-decorator-that-returns-a-method-of-the-same-class")
def parallel_decorator(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        data = args[0]

        data_chunks = split_strategy(data)(data)

        with mp.Pool() as pool:
            results = pool.starmap(partial(fn, **kwargs), ((i,) + args[1:] for i in data_chunks))
        return pd.concat(results).reset_index(drop=True)

    return wrapper


@logger
def task_runner(scheduled_tasks):
    """
    :return: sequential executing of parallel_runner
    """
    result = scheduled_tasks['data']

    with mp.Pool() as pool:
        for t in scheduled_tasks['tasks']:
            data_chunks = split_strategy(result)(result)
            results = pool.starmap(partial(t['task'], **t['kwargs']),
                                   ((i,) + tuple(t['args']) for i in data_chunks))
            result = pd.concat(results)
            print("Executing task: {} \n".format(t['task'].__name__))

    return result.reset_index(drop=True)


# TODO: as description
@logger
def dag_runner():
    """
    support defining a DAG to process the pandas dataframe to make it faster
    :return:
    """
    pass


@implicit_to_counter
def parallel_aggregate(dfGrouped, func):
    with mp.Pool() as pool:
        ret_list = pool.starmap(func, ((name, group) for name, group in dfGrouped))
    return pd.concat(ret_list)


@implicit_to_counter
def group_by_helper(groupby_df: pd.core.groupby.DataFrameGroupBy,
                    func: Callable[[Tuple[str, pd.DataFrame]], Union[pd.DataFrame, pd.Series]],
                    num_cpus: int = multiprocessing.cpu_count(),
                    logger: Callable[[str], None] = print) -> pd.DataFrame:
    start = time.time()
    logger("\nUsing {} CPUs in parallel...".format(num_cpus))
    with multiprocessing.Pool(num_cpus) as pool:
        queue = multiprocessing.Manager().Queue()
        result = pool.starmap_async(func, [(name, group) for name, group in groupby_df])
        cycler = itertools.cycle('\|/―')
        while not result.ready():
            logger("Percent complete: {:.0%} {}".format(queue.qsize() / len(groupby_df), next(cycler)), end="\r")
            time.sleep(0.4)
        got = result.get()
    logger("\nProcessed {} rows in {:.1f}s".format(len(got), time.time() - start))
    return pd.concat(got)


class TaskBuilder:
    """
    task = {
        'data': raw_df,
        'tasks': [
            {'task': flatten_array, 'args': ['col1'], 'kwargs':{}},
            {'task': flatten_any, 'args': ['col1'], 'kwargs':{}},
            {'task': flatten_any, 'args': ['col2'], 'kwargs':{}},
            {'task': flatten_array, 'args': ['col3'], 'kwargs':{}}
        ]
    }
    """

    def __init__(self, data):
        self._tasks = {
            'data': data,
            'tasks': []
        }

    def add_task(self, proc, *args, **kwargs):
        self._tasks['tasks'].append({'task': proc, 'args': args, 'kwargs': kwargs})
        return self

    def add_tasks(self, proc, args):
        for arg in args:
            self._tasks['tasks'].append({'task': proc, 'args': [arg], 'kwargs': {}})
        return self

    def set_data(self, data):
        self._tasks['data'] = data
        return self

    def build_task(self):
        return self._tasks
