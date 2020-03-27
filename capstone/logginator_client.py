import json
import requests
import logging

FORMAT='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s'

class LogginatorClient(logging.StreamHandler):
    
    url = ''
    
    def set_url(self, url):
        self.url = url


    def emit(self, record):
        formatter = logging.Formatter(FORMAT, "%Y-%m-%d %H:%M:%S")
        self.setFormatter(formatter)
        self.format(record)
        
        log_event = {
            'log_level': str(record.levelname),
            'message': str(record.message),
            'details': f"Line {str(record.lineno)}: {str(record.message)}",
            'source_application': str(record.module)
        }
        res = requests.post(self.url, json.dumps(log_event),
        headers={
            'Content-Type': 'application/json'
        })