"""
This is used to cover some basic python operation utils, such as json etc.
so in the future the dataframe operation should be moved to dataframe_util.py
though some aggregation operation offered by dataframe_util is now move to transformer.py
"""

import functools
import inspect
import time
import warnings
from functools import wraps

warnings.filterwarnings("ignore")

__all__ = ['safe_list_get',
           'get_by_path',
           'arr_str_wrapper',
           'str_wrapper',
           'normalize_json',
           'flatten_any',
           'flatten_array',
           'logger']


def safe_list_get(l, idx, default):
    """
    safe get array element by index, idx may be out of index, thus return default
    :param l:
    :param idx:
    :param default:
    :return:
    """
    try:
        return l[idx]
    except IndexError:
        return default
    except TypeError:
        return default


# TODO: implement this method
def set_dict_val(src, path, dst, sep="."):
    """
    :param src: {'a':'1'}
    :param path: a.b.c
    :param dst: 2
    :param sep:
    :return: {'a':{'b':{'c':2}}}
    """
    pass


def get_dict_val(deep_dict, path, default={}, sep="."):
    """
    Util function desgined to deal with nested json object.
    :param deep_dict: {"a":{"b":['1','2']}}
    :param path: 'a.b.[0]'
    :param default: {}
    :param sep: used to separate path
    :return: '1'
    """
    if len(path) == 0:
        return deep_dict

    if (not type(deep_dict) is dict) and (not type(deep_dict) is list):
        return default

    res = deep_dict
    for i in path.split(sep):
        if i[0] == '[' and i[-1] == ']':
            try:
                res = safe_list_get(res, int(i[1:-1]), default)
            except KeyError:
                return default
        else:
            try:
                res = res.get(i, default)
            except AttributeError:
                return default
    return res


def logger(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        ts = time.time()
        result = fn(*args, **kwargs)
        te = time.time()
        print("function      = {0}".format(fn.__name__))
        # print("    arguments = {0} {1}".format(args, kwargs))
        # print("    return    = {0}".format(result))
        print("    time      = %.6f sec" % (te - ts))
        return result

    return wrapper


def arr_str_wrapper(s):
    return ["'" + e + "'" for e in s]


def str_wrapper(s):
    return "'" + s + "'"


string_types = (type(b''), type(u''))


def deprecated(reason):
    """
    This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emitted
    when the function is used.
    """

    if isinstance(reason, string_types):

        # The @deprecated is used with a 'reason'.
        #
        # .. code-block:: python
        #
        #    @deprecated("please, use another function")
        #    def old_function(x, y):
        #      pass

        def decorator(func1):

            if inspect.isclass(func1):
                fmt1 = "Call to deprecated class {name} ({reason})."
            else:
                fmt1 = "Call to deprecated function {name} ({reason})."

            @functools.wraps(func1)
            def new_func1(*args, **kwargs):
                warnings.simplefilter('always', DeprecationWarning)
                warnings.warn(
                    fmt1.format(name=func1.__name__, reason=reason),
                    category=DeprecationWarning,
                    stacklevel=2
                )
                warnings.simplefilter('default', DeprecationWarning)
                return func1(*args, **kwargs)

            return new_func1

        return decorator

    elif inspect.isclass(reason) or inspect.isfunction(reason):

        # The @deprecated is used without any 'reason'.
        #
        # .. code-block:: python
        #
        #    @deprecated
        #    def old_function(x, y):
        #      pass

        func2 = reason

        if inspect.isclass(func2):
            fmt2 = "Call to deprecated class {name}."
        else:
            fmt2 = "Call to deprecated function {name}."

        @functools.wraps(func2)
        def new_func2(*args, **kwargs):
            warnings.simplefilter('always', DeprecationWarning)
            warnings.warn(
                fmt2.format(name=func2.__name__),
                category=DeprecationWarning,
                stacklevel=2
            )
            warnings.simplefilter('default', DeprecationWarning)
            return func2(*args, **kwargs)

        return new_func2

    else:
        raise TypeError(repr(type(reason)))


if __name__ == "__main__":
    print("test:")
    deep_dict= {"a":{"b":['1','2']}}
    print(get_dict_val(deep_dict, "a.b", ))
