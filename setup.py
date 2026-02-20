#!/usr/bin/env python

from setuptools import setup

setup(name='tap-azure-cloud-storage',
      version='0.0.1',
      description='Singer tap for extracting files from Azure Cloud Storage',
      author='Qlik',
      url='https://www.qlik.com',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      install_requires=[
          'backoff==2.2.1',
          'urllib3==2.2.1',
          'singer-encodings==0.4.0',
          'singer-python==6.1.1',
          'azure-storage-blob==12.19.0',
          'azure-identity==1.15.0',
          'adlfs==2024.2.0',
          'voluptuous==0.15.2'
      ],
      extras_require={
          'dev': [
              'ipdb',
              'pytest'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-azure-cloud-storage=tap_azure_cloud_storage:main
      ''',
      packages=['tap_azure_cloud_storage'])
