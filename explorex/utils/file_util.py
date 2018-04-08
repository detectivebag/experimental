"""
The deal with interaction with files and provide cache functionality to other functions through log these data on files
"""

import json
import os
import pickle
import re
from collections import Counter
from functools import wraps

from explorex.utils.dataframe_util import get_dict_item_cols, get_dict_cols


def save_json(filename, res):
    with open(filename, 'w') as outfile:
        json.dump(res, outfile)


def load_json(filename):
    with open(filename, 'r') as f:
        res = json.load(f)
    return res


def generate_cache_filename(kwargs):
    str_args = [str(k) for k in kwargs]
    seps = re.sub('[=\\\\/:*?"<>|\s()&,{}.\[\]\'\"]', "_", "_".join(str_args)).split("_")
    return ("_".join(filter(lambda e: not e == '' and not e == ' ', seps)))[0:249] + ".json"


def load_many_json(abs_dir, file_list):
    res = []
    for filename in file_list:
        with open(abs_dir + filename, 'r') as f:
            res += json.load(f)
    return res


def fetch_from_cache(fetch_data_fun, *kwargs):
    force = True in kwargs
    cache_file = generate_cache_filename(kwargs)
    if os.path.exists(cache_file) and not force:
        print("loading data from cache file: " + cache_file)
        return load_json(cache_file)
    else:
        data = fetch_data_fun(*kwargs)
        res = [d.get('_source', d) for d in data]
        save_json(cache_file, res)
        print("cache_file: " + cache_file + " is generated!")
        return res


def file_cache(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        force = kwargs.pop('force_fetch', False)
        no_cache = kwargs.pop('no_cache', False)

        if no_cache:
            data = fn(*args, **kwargs)
            return data

        cache_file = generate_cache_filename(args)
        print("lookup data from cache file: " + cache_file)
        if os.path.exists(cache_file) and not force:
            print("loading data from cache file: " + cache_file)
            return load_json(cache_file)
        else:
            data = fn(*args, **kwargs)
            res = [d.get('_source', d) for d in data]
            save_json(cache_file, res)
            print("cache_file: " + cache_file + " is generated!")
            return data

    return wrapper


def save_pickle(obj, filename):
    with open(filename, 'wb') as f:
        pickle.dump(obj, f)


def load_pickle(filename):
    with open(filename, 'rb') as f:
        res = pickle.load(f)
    return res


def save_df(df, filename):
    dict_item_type = type({'1':1}.items())
    df_copy = df.copy()
    item_cols = get_dict_item_cols(df)
    for i in item_cols:
        df_copy[i] = df_copy[i].map(lambda e: dict(e) if type(e) is dict_item_type else e)
    save_pickle(df_copy, filename)


def load_df(filename):
    df = load_pickle(filename)
    dict_cols = get_dict_cols(df)
    for i in dict_cols:
        df[i] = df[i].map(lambda e: Counter(e).items() if type(e) is dict else e)
    return df
