import logging

logging.basicConfig(filename='example.log', level=logging.DEBUG)
logging.debug('This message should go to he log file')
logging.info('So should this')
logging.warning('And this too')