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
        'Flask',
        'libmc',
        'pyyaml',
        'kazoo',
        'mmh3',
    ],
    ext_modules=[
        Extension('beansdbadmin.core.fnv1a', ['beansdbadmin/core/fnv1a.c']),
    ],
    entry_points={
        'console_scripts': [
            'beansdbadmin-server = beansdbadmin.index:main',
            'beansdb-gc = beansdbadmin.tools.gc:main',
            'beansdb-logreport = beansdbadmin.tools.logreport:main',
            # 'beansdb-admin-agent = beansdbadmin.core.agent:main',
            # 'beansdb-agent-cli = beansdbadmin.core.agent_cli:main',
            # 'beansdb-dump-data = beansdbadmin.core.data:main',
            # 'beansdb-dump-hint = beansdbadmin.core.hint:dump_hint',
        ],
    }
)
