import logging


def basicConfig():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s:%(lineno)d %(levelname)s %(message)s',
                        datefmt='%Y%m%dT%H:%M:%S')
