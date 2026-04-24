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
          'urllib3==2.6.3',
          'singer-encodings==0.5.0',
          'singer-python==6.8.0',
          'azure-storage-blob==12.28.0',
          'azure-identity==1.25.3',
          'adlfs==2026.2.0',
          'voluptuous==0.16.0'
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
