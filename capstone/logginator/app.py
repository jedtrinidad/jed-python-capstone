import os
import json
import logging
import requests
import uuid

import boto3
from botocore.exceptions import ClientError


logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s')
log = logging.getLogger()


info_arn = os.getenv('TOPIC_INFO_ARN')
debug_arn = os.getenv('TOPIC_DEBUG_ARN')
warning_arn = os.getenv('TOPIC_WARNING_ARN')
error_arn = os.getenv('TOPIC_ERROR_ARN')
critical_arn = os.getenv('TOPIC_CRITICAL_ARN')


def lambda_handler(event, context):
    log_event = {
        'log_level': event['log_level'],
        'message': event['message'],
        'details': event['details'],
        'source_application': event['source_application']
    }
    log.info(f"Recieved Log Event: {json.dumps(log_event)}")
    return process_log_events(log_event)


def publish_sns_message(topic_arn, message):
    sns = boto3.client('sns')
    params = {
        'TopicArn': topic_arn,
        'Message': message,
        'MessageStructure': 'json'
    }
    return sns.publish(**params)


def save_to_ddb(log_event):
    # TODO: implementation
    return log_event


def send_critical_email(log_event):
    if os.getenv('DEVOPS_EMAIL'):
        payload = {
            "to": os.getenv('DEVOPS_EMAIL'),
            "subject": f"CRITICAL ERROR @ {log_event['source_application']}",
            "body": json.dumps(log_event)
        }
        url = os.getenv('EMAIL_SENDER_API')
        requests.post(url, json=payload)
        return True
    else:
        return False


def process_log_events(log_event):
    if log_event['log_level'] == 'INFO':
        message = json.dumps({'default' : json.dumps(log_event)})
        publish_sns_message(info_arn, message)
    elif log_event['log_level'] == 'DEBUG':
        message = json.dumps({'default' : json.dumps(log_event)})
        publish_sns_message(debug_arn, message)
    elif log_event['log_level'] == 'WARNING':
        message = json.dumps({'default' : json.dumps(log_event)})
        publish_sns_message(warning_arn, message)
    elif log_event['log_level'] == 'ERROR':
        message = json.dumps({'default' : json.dumps(log_event)})
        publish_sns_message(error_arn, message)
    elif log_event['log_level'] == 'CRITICAL':
        message = json.dumps({'default' : json.dumps(log_event)})
        publish_sns_message(critical_arn, message)
        send_critical_email(log_event)
    else:
        return {
            'statusCode': 400,
            'message': "Invalid Log Level"
        }
    return {
        'statusCode': 200,
        'logEvent': log_event
    }

