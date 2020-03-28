# Capstone Project: Logginator


## Introduction
Logginator is a logging application developed using Python and deployed as a function 
in AWS Lambda. Logginator can be used to log multiple python application using the 
`LogginatorClient`.

## Architecture
The function can recieve requests via AWS API Gateway. The API validates the request body,
before it reaches the function. This was done using JSON Schema. Doing this simplifies the code.

Logs are stored in a DynamoDB table. The primary key consists of `log_level` and `timestamp`.

## Code Sample

To add `LogginatorClient` to a script.
`
import logging

from logginator_client import LogginatorClient
# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s'
    )


log = logging.getLogger(__name__)
log_url = '''
https://3tdgwj7eog.execute-api.ap-southeast-1.amazonaws.com/beta/logs
'''
client = LogginatorClient()
client.set_url(log_url)
log.addHandler(client)`
`
