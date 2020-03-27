import sys
import logging

import boto3
from botocore.exceptions import ClientError


logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s')
log = logging.getLogger()


def create_sns_topic(topic_name):
    sns = boto3.client('sns')
    sns.create_topic(Name=topic_name)
    return True


def list_sns_topic(next_token=None):
    sns = boto3.client('sns')
    params = {'NextToken': next_token} if next_token else {}
    topics = sns.list_topics(**params)
    return topics.get('Topics', {}), topics.get('NextToken', None)


def list_sns_subscriptions(next_token=None):
    sns = boto3.client('sns')
    params = {'NextToken': next_token} if next_token else {}
    subscriptions = sns.list_subscriptions(**params)
    return subscriptions.get('Subscriptions', []), subscriptions.get('NextToken', None)


def subscribe_sns_topic(topic_arn, mobile_number):
    sns = boto3.client('sns')
    params = {
        'TopicArn': topic_arn,
        'Protocol': 'sms',
        'Endpoint': mobile_number,
    }
    res = sns.subscribe(**params)
    print(res)
    return True


def send_sns_message(topic_arn, message):
    sns = boto3.client('sns')
    params = {
        'TopicArn': topic_arn,
        'Message': message,
    }
    res = sns.publish(**params)
    print(res)
    return True


def unsubscribe_sns_topic(subscription_arn):
    sns = boto3.client('sns')
    params = {
        'SubscriptionArn': subscription_arn,
    }
    res = sns.unsubscribe(**params)
    print(res)
    return True


def delete_sns_topic(topic_arn):
    sns = boto3.client('sns')
    sns.delete_topic(TopicArn=topic_arn)
    return True


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers(
        title='Commands'
    )
    
    sp_create_sns_topic = subparser.add_parser(
        'create-sns-topic'
    )
    
    sp_create_sns_topic.add_argument(
        'topic_name'
    )
    
    sp_create_sns_topic.set_defaults(func=create_sns_topic)
    
    sp_list_sns_topics = subparser.add_parser(
        'list-sns-topics'
    )
    
    sp_list_sns_topics.add_argument(
        '--next-token', '-N'
    )
    
    sp_list_sns_topics.set_defaults(func=list_sns_topic)
    
    sp_list_sns_subscriptions = subparser.add_parser(
        'list-sns-subscriptions'
    )
    
    sp_list_sns_subscriptions.add_argument(
        '--next-token', '-N'
    )
    
    sp_list_sns_subscriptions.set_defaults(func=list_sns_subscriptions)
    
    sp_subscribe_sns_topic = subparser.add_parser(
        'subscribe-sns-topic'
    )
    
    sp_subscribe_sns_topic.add_argument(
        'topic_arn'
    )
    
    sp_subscribe_sns_topic.add_argument(
        'mobile_number'
    )
    
    sp_subscribe_sns_topic.set_defaults(func=subscribe_sns_topic)
    
    sp_send_sns_message = subparser.add_parser(
        'send-sns-message'
    )
    
    sp_send_sns_message.add_argument(
        'topic_arn'
    )
    
    sp_send_sns_message.add_argument(
        'message'
    )
    
    sp_send_sns_message.set_defaults(func=send_sns_message)
    
    sp_unsubscribe_sns_topic = subparser.add_parser(
        'unsubscribe-sns-topic'
    )
    
    sp_unsubscribe_sns_topic.add_argument(
        'subscription_arn'
    )
    
    sp_unsubscribe_sns_topic.set_defaults(func=unsubscribe_sns_topic)
    
    sp_delete_sns_topic = subparser.add_parser(
        'delete-sns-topic'
    )
    
    sp_delete_sns_topic.add_argument(
        'topic_arn'
    )
    
    sp_delete_sns_topic.set_defaults(func=delete_sns_topic)
    
    pargs = parser.parse_args()
    action = pargs.func.__name__ if hasattr(pargs, 'func') else ''
    
    if action == 'create_sns_topic':
        print(pargs.func(pargs.topic_name))
        sys.exit(0)
    elif action == 'list_sns_topic':
        print(pargs.func(pargs.next_token))
        sys.exit(0)
    elif action == 'list_sns_subscriptions':
        print(pargs.func(pargs.next_token))
        sys.exit(0)
    elif action == 'subscribe_sns_topic':
        pargs.func(pargs.topic_arn, pargs.mobile_number)
        sys.exit(0)
    elif action == 'send_sns_message':
        pargs.func(pargs.topic_arn, pargs.message)
        sys.exit(0)
    elif action == 'unsubscribe_sns_topic':
        pargs.func(pargs.subscription_arn)
        sys.exit(0)
    elif action == 'delete_sns_topic':
        print(pargs.func(pargs.topic_arn))
        sys.exit(0)
    else:
        print('Invalid or Missing Command.')
        sys.exit(1)
