import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s')
    
logging.debug('This is a debug message')
logging.info('This is an info message')
logging.warning('This is a warning')