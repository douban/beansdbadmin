from setuptools import setup


setup(
    name='beansdbadmin',
    version='1.0',
    description='BeansDB Admin',
    author='zhuzhaolong,yangxiufeng',
    author_email='zhuzhaolong@douban.com,yangxiufeng@douban.com',
    url='http://github.intra.douban.com/coresys/beansdbadmin',
    packages=[
        'beansdbadmin',
        'beansdbadmin.tools'
    ],
    package_data={
        'beansdbadmin': [
            'static/*/*',
            'templates/*',
        ]
    },
    entry_points = {
        'console_scripts': [
            'beansdbadmin-gc = beansdbadmin.tools.gc:main',
            'beansdbadmin-web = beansdbadmin.index:main',
        ]
    },
)
