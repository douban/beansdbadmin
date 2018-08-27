#!/usr/bin/env python
# coding: utf-8

from setuptools import setup, find_packages, Extension

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
    url='https://github.com/douban/beansdbadmin',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'Flask==0.10.0',
        'libmc==1.1.0',
        'pyyaml==3.12',
        'kazoo==2.2.1',
        'mmh3==2.5.1',
    ],
    ext_modules=[
        Extension('beansdbadmin.core.fnv1a', ['beansdbadmin/core/fnv1a.c']),
    ],
    entry_points={
        'console_scripts': [
            'beansdbadmin-server = beansdbadmin.index:main',
            # 'beansdb-admin-agent = beansdbadmin.core.agent:main',
            # 'beansdb-agent-cli = beansdbadmin.core.agent_cli:main',
            # 'beansdb-dump-data = beansdbadmin.core.data:main',
            # 'beansdb-dump-hint = beansdbadmin.core.hint:dump_hint',
        ],
    }
)
