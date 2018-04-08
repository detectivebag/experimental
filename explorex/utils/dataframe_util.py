from collections import Counter
from functools import wraps

import pandas as pd
import numpy as np


def normalize_json(j):
    df = pd.io.json.json_normalize(j)
    return rename_dataframe(df)


def parse_col_name(n):
    l = n.split('.')
    if len(l) > 1 and l[0] == '_source':
        return ".".join(l[1:])
    else:
        return n


def rename_dataframe(df):
    names = [parse_col_name(i) for i in df.columns]
    df.columns = names
    return df


def get_cols_by_type(df, col_type):
    """
    :param df:
    :param col_type:
    :return:
    """
    fields = []
    for i in df.columns.tolist():
        if safe_get_type(df, i) is col_type:
            fields.append(i)
    return fields


def get_list_cols(df):
    """
    return the column names list whose values type is list
    """
    return get_cols_by_type(df, list)


def get_dict_cols(df):
    """
    return the column names list whose values types is dict
    """
    return get_cols_by_type(df, dict)


def get_dict_item_cols(df):
    return get_cols_by_type(df, type({'1': 1}.items()))


def get_str_cols(df):
    return get_cols_by_type(df, str)


def flatten_array(df, col):
    """
    this function only works on array-like data
    toConvert = pd.DataFrame({
        'x': [1, 2],
        'y': [10, 20],
        'z': [(101, 102, 103), (201, 202)]
    })
    flatten_data(toConvert, 'z')
    :param df: pandas dataframe
    :param col: the column you want to flatten
    :return: dataframe with flattened field
    """
    # if the dataframe does not contain the specific column then should return the original one
    if (col not in df.columns) or (not safe_get_type(df, col) is list):
        return df

    tmp = []
    cols = df.columns

    def backend(r):
        m = {}
        for c in cols:
            if not c == col:
                m[c] = r[c]
        zz = r[col]

        if not type(zz) is list or len(zz) == 0:
            tmp.append(m)
        else:
            for z in zz:
                tmp.append({**m, **{col: z}})

    df.apply(backend, axis=1)
    return pd.DataFrame(tmp)


def safe_get_type(df, col):
    """
    this function will show the type of the column for given dataframe
    :param df: the pandas dataframe
    :param col: the column to be checked for its type
    :return: the actual type of the specific column
    """
    try:
        return type(df[col][df[col].notnull()].iloc[0])
    except IndexError:
        return type(np.nan)


def flatten_any(df, col=None, prefix="flattened"):
    """
    this deals with two kinds of data: 1 array-like data 2 object-like data
    :param prefix: prefix if the col is not set
    :param df: dataframe to be changed
    :param col: column to be flattened
    :return: the flattened dataframe(the selected field will be flattened first and then
    merged into the origin dataframe)
    """
    # only flatten the column if that column is array-like or object-like
    if col is None:
        return df.apply(pd.Series).add_prefix(prefix + ".")

    t = safe_get_type(df, col)

    if t is list or t is dict:
        # use pd.concat is important, df.join cause data inconsistency
        return pd.concat([df, df[col].apply(pd.Series).add_prefix(col + ".")], axis=1).drop(col, 1)
    else:
        return df


def implicit_to_dict(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        df = fn(*args, **kwargs)
        dict_item_type = type({'1': 1}.items())
        df_copy = df.copy()
        item_cols = get_dict_item_cols(df)
        for i in item_cols:
            df_copy[i] = df_copy[i].map(lambda e: dict(e) if type(e) is dict_item_type else e)
        return df_copy

    return wrapper


def implicit_to_counter(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        df = fn(*args, **kwargs)
        dict_cols = get_dict_cols(df)
        for i in dict_cols:
            df[i] = df[i].map(lambda e: Counter(e).items() if type(e) is dict else e)
        return df

    return wrapper


def df_cols_sub(df, cols):
    all_cols = set(df.columns)
    sub_cols = set(cols)
    return list(all_cols - sub_cols)
