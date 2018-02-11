import os

from setuptools import setup


def is_package(path):
    return (
        os.path.isdir(path) and
        os.path.isfile(os.path.join(path, '__init__.py'))
    )


def find_packages(path, base=''):
    """Find all packages in path """
    packages = {}
    for item in os.listdir(path):
        dir = os.path.join(path, item)
        if is_package(dir):
            if base:
                module_name = base + '.' + item
            else:
                module_name = item
            packages[module_name] = dir
            packages.update(find_packages(dir, module_name))
    return packages


def readme():
    with open('README.md') as f:
        return f.read()


setup(name='explorex',
      version='0.0.1',
      description='Python data analytics in functional programming way.',
      long_description=readme(),
      author='J.J. Young',
      author_email='flyera2010@gmail.com',
      license='MIT',
      packages=find_packages("."),
      zip_safe=False,
      include_package_data=True,
      url='https://github.com/detectivebag/experimental.git')