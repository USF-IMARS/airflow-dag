#!/usr/bin/env python
""" setup.py for IMaRS Airflow DAGs """

from setuptools import setup
import io

import imars_dags

def read(*filenames, **kwargs):
    encoding = kwargs.get('encoding', 'utf-8')
    sep = kwargs.get('sep', '\n')
    buf = []
    for filename in filenames:
        with io.open(filename, encoding=encoding) as f:
            buf.append(f.read())
    return sep.join(buf)

long_description = read('README.md') #, 'CHANGES.txt')

setup(name='imars_dags',
    version=imars_dags.__version__,
    description='USF IMaRS Airflow DAGs',
    long_description=long_description,
    author='Tylar Murray',
    author_email='imarsroot@marine.usf.edu',
    url='https://github.com/USF-IMARS/imars_dags',

    tests_require=['nose'],
    install_requires=[
        'git+https://git}@github.com/7yl4r/pycmr.git/@prod'
        # TODO: add imars-etl here?
    ],
    #cmdclass={'test': PyTest},

    packages=['imars_dags']
)
