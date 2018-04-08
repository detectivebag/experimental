import configparser
import os
import socket
import sys
import yaml


def get_hostname():
    return socket.gethostname()


def get_env():
    hostname = get_hostname()
    if 'sha' in hostname:
        return 'test'
    elif 'dev' in hostname:
        return 'dev'
    elif 'stg' in hostname or 'stage' in hostname:
        return 'stg'
    elif 'prod' in hostname or 'prd' in hostname:
        return 'prd'
    else:
        return 'test'


def read_config(config_file_path, field, key):
    cf = configparser.ConfigParser()
    try:
        cf.read(config_file_path)
        result = cf.get(field, key)
    except:
        sys.exit(1)
    return result


def write_config(config_file_path, field, key, value):
    cf = configparser.ConfigParser()
    try:
        cf.read(config_file_path)
        cf.set(field, key, value)
        cf.write(open(config_file_path, 'w'))
    except:
        sys.exit(1)
    return True


class Config:
    def __init__(self, path):
        self.path = path
        self.cf = configparser.ConfigParser()
        self.cf.read(self.path)
        self.env = get_env()

    def get(self, field, key):
        try:
            result = self.cf.get(field, key)
        except:
            result = ""
        return result

    def set(self, field, key, value):
        try:
            self.cf.set(field, key, value)
            with open(self.path, 'w') as f:
                self.cf.write(f)
        except:
            return False
        return True


class PortConfig(Config):
    def __init__(self, path=None):
        super().__init__(path or os.path.join(os.path.dirname(os.path.abspath(__file__)), "port.ini"))

    def get_user(self):
        return self.get('pass', 'uid')

    def get_passwd(self):
        return self.get('pass', 'pass')


class YamlConfig:
    def __init__(self, path):
        self._path = path
        with open(path, 'r') as ymlfile:
            self._cfg = yaml.load(ymlfile)

    def get(self, key):
        return self._cfg[key]

    def set(self, key, val):
        self._cfg[key] = val
        with open(self._path, 'w') as ymlfile:
            ymlfile.write(yaml.dump(self._cfg, default_flow_style=False))


class MLFeatureDefConf:
    def __init__(self, attr_path=None, feature_path=None):
        self._attr = YamlConfig(
            attr_path or os.path.join(os.path.dirname(os.path.abspath(__file__)), "feature_transform_map.yaml"))
        self._features = YamlConfig(
            feature_path or os.path.join(os.path.dirname(os.path.abspath(__file__)), "feature_list.yaml"))

    def get_attribute_def(self, key=None):
        return self._attr.get(key or 'transform_map')

    def get_feature_desc(self, key=None):
        return self._features.get(key or 'feature_list')


if __name__ == "__main__":
    # conf = PortConfig()
    # print(conf.get_passwd())
    ml = MLFeatureDefConf()
    print(ml.get_attribute_def())
    print(ml.get_feature_desc())
