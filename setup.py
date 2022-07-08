import os
from distutils.core import setup
from setuptools import find_packages

with open(os.path.join(os.path.dirname(__file__), 'requirements.txt')) as f:
    required = f.read().splitlines()

setup(name='traderbase',
      version='0.0.22',
      description='daytrader base trader lib',
      author='day-trader',
      author_email='david.monllao@gmail.com',
      url='https://github.com/datarellie/traderbase',
      packages=find_packages(where='src'),
      package_dir={'': 'src'},
      install_requires=required
     )
