from setuptools import setup

setup(
    name='beansdbadmin',
    version='1.0',
    description='BeansDB Admin',
    author='zhuzhaolong,yangxiufeng',
    author_email='zhuzhaolong@douban.com,yangxiufeng@douban.com',
    url='http://github.intra.douban.com/coresys/beansdbadmin',
    packages=['beansdbadmin'],
    entry_points = """
    [console_scripts]
    beansdbadmin-agent=beansdbadmin.agent:main
    """
)