#!/usr/bin/env python

from setuptools import setup

setup(name='tap-gnip',
      version='0.0.1',
      description='Singer.io tap for extracting data from the GNIP API',
      author='Fishtown Analytics',
      url='http://fishtownanalytics.com',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_gnip'],
      install_requires=[
          'singer-python==5.0.12',
          'backoff==1.3.2',
          'requests==2.18.4',
          'requests-oauthlib==0.8.0',
          'funcy==1.10.1',
      ],
      dependency_links=[
          '-e ../tap-framework',
      ],
      entry_points='''
          [console_scripts]
          tap-gnip=tap_gnip:main
      ''',
      packages=['tap_gnip'])
