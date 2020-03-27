import logging
import random
import uuid
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path, PosixPath

import boto3
from botocore.exceptions import ClientError


logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s')
log = logging.getLogger()


def list_log_groups(group_name=None, region_name=None):
    cwlogs = boto3.client('logs', region_name=region_name)
    params = {
        'logGroupNamePrefix': group_name,
    } if group_name else {}
    res = cwlogs.describe_log_groups(**params)
    return res['logGroups']


def list_log_group_streams(group_name, stream_name=None, region_name=None):
    cwlogs = boto3.client('logs', region_name=region_name)
    params = {
        'logGroupName': group_name,
    } if group_name else {}
    if stream_name:
        params['logStreamNamePrefix'] = stream_name
    res = cwlogs.describe_log_streams(**params)
    return res['logStreams']


def filter_log_events(
    group_name, filter_pat,
    start=None, stop=None,
    region_name=None
):
    cwlogs = boto3.client('logs', region_name=region_name)
    params = {
        'logGroupName': group_name,
        'filterPattern': filter_pat,
    }
    if start:
        params['startTime'] = start
    if stop:
        params['endTime'] = stop
    res = cwlogs.filter_log_events(**params)
    return res['events']


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(
        title='Commands'
    )
    
    
    # List Log Groups
    sp_list_log_groups = subparsers.add_parser(
        'list-log-groups',
        help='List all log groups.'
    )
    
    sp_list_log_groups.add_argument(
        '--group-name', '-G',
        help='Name of log group.'
    )
    
    sp_list_log_groups.add_argument(
        'region_name',
        help='Region where logs are stored.'
    )
    
    sp_list_log_groups.set_defaults(func=list_log_groups)
    
    
    # List Log Group Streams
    sp_list_log_group_streams = subparsers.add_parser(
        'list-log-group-streams',
        help='List Streams in a log group.'
    )
    
    sp_list_log_group_streams.add_argument(
        'group_name'
    )
    
    sp_list_log_group_streams.add_argument(
        '--stream-name', '-N',
        help='The Name of a Stream.'
    )
    
    sp_list_log_group_streams.add_argument(
        '--region-name', '-R',
        help='The Name of the region where the stream is stored.'
    )
    
    sp_list_log_group_streams.set_defaults(func=list_log_group_streams)
    
    
    # Filter Log Events
    sp_filter_log_events = subparsers.add_parser(
        'filter-log-events',
        help='Filter Log Events in a log group'
    )
    
    sp_filter_log_events.add_argument(
        'group_name'
    )
    
    sp_filter_log_events.add_argument(
        'filter_pattern'
    )
    
    sp_filter_log_events.add_argument(
        '--start', '-S',
        type=lambda s: 
            int(datetime.strptime(s , '%Y-%m-%d %H:%M:%S').timestamp() * 1000) 
    )
    
    sp_filter_log_events.add_argument(
        '--end', '-E',
        type=lambda s: 
            int(datetime.strptime(s , '%Y-%m-%d %H:%M:%S').timestamp() * 1000) 
    )
    
    sp_filter_log_events.add_argument(
        '--region-name', '-R'
    )
    
    sp_filter_log_events.set_defaults(func=filter_log_events)
    
    pargs = parser.parse_args()
    action = pargs.func.__name__ if hasattr(pargs, 'func') else ''
    
    if action == 'list_log_groups':
        print(pargs.func(pargs.group_name, pargs.region_name))
        sys.exit(0)
    elif action == 'list_log_group_streams':
        print(pargs.func(pargs.group_name, pargs.stream_name, pargs.region_name))
        sys.exit(0)
    elif action == 'filter_log_events':
        print(pargs.func(
            pargs.group_name, pargs.filter_pattern,
            pargs.start, pargs.end, pargs.region_name
            )
        )
        sys.exit(0)
    else:
        print("Missing or Invalid Command.")
        sys.exit(1)

    