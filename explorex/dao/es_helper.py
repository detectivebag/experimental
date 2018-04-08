from functools import partial

from elasticsearch import Elasticsearch
from elasticsearch import helpers

from .data_tunnel import BasicTunnel, PortMappingConf
from ..utils.operator_util import file_cache

"""
This is designed for dealing with data related to testbed ES, since those servers are not blocked.
"""


@file_cache
def scan_data(hostname, es_index, es_type, query):
    """
    batch scan data from ES
    :param hostname:
    :param es_index:
    :param es_type:
    :param query:
    :return:
    """
    es = Elasticsearch([hostname])

    page = es.search(
        index=es_index,
        doc_type=es_type,
        scroll='2m',
        search_type='scan',
        size=query.get('size') or 1000,
        body=query)
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']

    res = []
    # Start scrolling
    while scroll_size > 0:
        print("Scrolling...")
        page = es.scroll(scroll_id=sid, scroll='2m')
        # Update the scroll ID
        sid = page['_scroll_id']
        # Get the number of results that we returned in the last scroll
        scroll_size = len(page['hits']['hits'])
        res += page['hits']['hits']
        print("scroll size: " + str(scroll_size))

    return res


def batch_insert_data(hostname, es_index, es_type, data):
    """
    batch insert data into ES
    :param hostname: e.g. 'xxx-host-test-06'
    :param es_index: e.g. 'some_index'
    :param es_type: e.g. 'some_type'
    :param data: python build in array-like data, each is an ES record
    :return:
    """
    es = Elasticsearch([hostname])
    actions = [
        {"_index": es_index, "_type": es_type, "_id": d.get('_id'), "_source": d.get('_source')} for d
        in data]
    helpers.bulk(es, actions)


def search_data(hostname, es_index, es_type, query=None):
    if query is None:
        query = {"query": {"match_all": {}}}
    es = Elasticsearch([hostname])
    res = es.search(index=es_index, doc_type=es_type, body=query)
    return res['hits']['hits']


def fetch_data(hostname, es_index, es_type, es_id):
    es = Elasticsearch([hostname])
    return es.get(es_index, es_id, doc_type=es_type)


class ESDataProvider:
    _es_type_index = [
        ('type_1', 'index_2'),
        ('type_2', 'type_2'),
        ('type_3', 'type_3')
    ]

    def __init__(self, env):
        self._tunnel_conf = PortMappingConf(env, 'es')
        self._es = Elasticsearch(["{0}:{1}".format(self._tunnel_conf.local_addr, self._tunnel_conf.local_addr_port)])
        self._create_attr()

    @property
    def tunnel_conf(self):
        return self._tunnel_conf

    @BasicTunnel
    def _get(self, es_index, es_type, es_id):
        return self._es.get(es_index, es_id, doc_type=es_type)

    @BasicTunnel
    def _search(self, es_index, es_type, es_query):
        return self._es.search(index=es_index, doc_type=es_type, body=es_query)

    @BasicTunnel
    def _index(self, index, doc_type, body, id=None):
        return self._es.index(index, doc_type, body, id=id, params=None)

    @BasicTunnel
    def _scan(self, es_index, es_type, query):
        page = self._es.search(
            index=es_index,
            doc_type=es_type,
            scroll='2m',
            search_type='scan',
            size=query.get('size') or 1000,
            body=query)
        sid = page['_scroll_id']
        scroll_size = page['hits']['total']

        res = []
        while scroll_size > 0:
            print("Scrolling...")
            page = self._es.scroll(scroll_id=sid, scroll='2m')
            sid = page['_scroll_id']
            scroll_size = len(page['hits']['hits'])
            res += page['hits']['hits']
            print("scroll size: " + str(scroll_size))

        return res

    @BasicTunnel
    def _bulk(self, es_index, es_type, data):
        actions = [
            {"_index": es_index,
             "_type": es_type,
             "_id": d.get('_id'),
             "_source": d.get('_source')} for d in data]
        helpers.bulk(self._es, actions)

    def _create_attr(self):
        for conf in self._es_type_index:
            for fn in ['search', 'scan', 'bulk', 'get']:
                setattr(self, '{}_{}_{}'.format(fn, conf[0], conf[1]),
                        partial(getattr(self, "_%s" % fn), conf[0], conf[1]))
