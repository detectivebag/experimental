"""
more detail about class decorating, please view following page:
https://www.codementor.io/sheena/advanced-use-python-decorators-class-function-du107nxsv
"""


from sshtunnel import SSHTunnelForwarder

from ..conf import PortConfig


class PortMappingConf:
    _mapping = {
        'dev': {
            'es': ['xxx-dev-host', 'xxx-dev-mysql', 9200],
            'mysql': ['xxx-dev-host', 'xxx-dev-mysql', 14306],
            'redis': ['xxx-dev-host', 'xxx-dev-redis', 6379]
        },
        'stg': {
            'es': ['xxx-stage-host', 'xxx-stage-mysql', 9200],
            'mysql': ['xxx-stage-host', 'xxx-stage-mysql', 14306],
            'redis': ['xxx-stage-host', 'xxx-dev-redis', 6379]
        },
        'prd': {
            'es': ['xxx-prod-host', 'xxx-prod-es-01', 9200],
            'mysql': ['xxx-prod-host', 'xxx-prod-mysql', 14306],
            'redis': ['xxx-prod-host', 'xxx-prod-redis', 6379]
        }

    }

    def __init__(self, env, source):
        self.relay_dst_addr, self.dst_addr, self.dst_addr_port = self._mapping[env][source]
        self.local_addr = '127.0.0.1'
        self.local_addr_port = self._get_open_port()

    def __str__(self):
        return "relay address:       {}\n" \
               "destination address: {}\n" \
               "destination port:    {}\n" \
               "local address:       {}\n" \
               "local address port:  {}\n".format(self.relay_dst_addr, self.dst_addr, self.dst_addr_port,
                                                  self.local_addr, self.local_addr_port)

    __repr__ = __str__

    @staticmethod
    def _get_open_port():
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("", 0))
        s.listen(1)
        port = s.getsockname()[1]
        s.close()
        return port

    @property
    def mapping(self):
        return self._mapping


class BasicTunnel:
    """
    this is used to decorate method of class.
    e.g.:
    class A:
        @BasicTunnel
        def foo():
            pass
    """
    _port_conf = PortConfig()
    _user = _port_conf.get_user()
    _pass = _port_conf.get_passwd()

    def __init__(self, func):
        self._func = func

    def __call__(self, *args, **kwargs):
        port_map = self.obj.tunnel_conf
        with SSHTunnelForwarder(
                port_map.relay_dst_addr,
                ssh_username=self._user,
                ssh_password=self._pass,
                local_bind_address=(port_map.local_addr, port_map.local_addr_port),
                remote_bind_address=(port_map.dst_addr, port_map.dst_addr_port)
        ) as server:
            server.start()
            res = self._func(self.obj, *args, **kwargs)
            server.stop()
        return res

    def __get__(self, instance, owner):
        self.cls = owner
        self.obj = instance

        return self.__call__