#!/usr/bin/env python
# coding: utf-8

from setuptools import setup, find_packages

version = '0.1'

setup(
    name='beansdbadmin',
    version=version,
    description='admin for gobeansdb',
    long_description="""admin for gobeansdb""",
    classifiers=[],
    keywords='',
    author='Douban Inc.',
    author_email='platform@douban.com',
    url='',
    license='',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'Flask==0.10.0',
        'beansdb-tools',
    ],
    dependency_links=[
        "git+https://github.intra.douban.com/coresys/beansdb-tools.git@master#egg=beansdb-tools",
    ],
    tests_require=[
    ],
)
