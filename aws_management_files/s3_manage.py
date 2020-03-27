import logging
import random
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path, PosixPath

import boto3
from botocore.exceptions import ClientError


logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s')
log = logging.getLogger()


def create_bucket(name, region=None):
    region = region or 'ap-southeast-1'
    client = boto3.resource('s3', region_name=region)
    params = {
        'Bucket': name,
        'CreateBucketConfiguration': {
            'LocationConstraint': region,
        }
    }
    
    try:
        client.create_bucket(**params)
        return True
    except ClientError as err:
        log.error(f'{err} - Params {params}')
        return False


def list_buckets():
    s3 = boto3.resource('s3')
    
    count = 0
    for bucket in s3.buckets.all():
        print(bucket.name)
        count += 1
    
    print(f"Found {count} buckets")


def get_bucket(name, create=False, region=None):
    client = boto3.resource('s3')
    bucket = client.Bucket(name=name)
    
    if bucket.creation_date:
        return bucket
    else:
        if create:
            create_bucket(name, region=region)
            return get_bucket(name)
        else:
            log.warning(f'Bucket {name} does not exist.')


def create_bucket_object(bucket_name, file_path, key_prefix=None):
    bucket = get_bucket(bucket_name)
    dest = f'{key_prefix or ""}{file_path}'
    bucket_object = bucket.Object(dest)
    bucket_object.upload_file(Filename=file_path)
    return bucket_object


def get_bucket_object(bucket_name, object_key, dest=None, version_id=None):
    bucket = get_bucket(bucket_name)
    params = {'key': object_key}
    if version_id:
        params['VersionId'] = version_id
    bucket_object = bucket.Object(**params)
    dest = Path(f'{dest or ""}')
    file_path = dest.joinpath(PosixPath(object_key).name)
    bucket_object.download_file(f'{file_path}')
    return bucket_object, file_path


def enable_bucket_versioning(bucket_name):
    bucket = get_bucket(bucket_name)
    versioned = bucket.Versioning()
    versioned.enable()
    return versioned.status


def list_bucket_objects(bucket_name, prefix=None):
    bucket = get_bucket(bucket_name)
    if prefix:
        return bucket.objects.filter(Prefix=prefix)
    return bucket.objects.all()


def delete_bucket_object(bucket_name, object_key):
    bucket = get_bucket(bucket_name)
    bucket.Object(object_key).delete()


def delete_bucket_objects(bucket_name, key_prefix):
    bucket = get_bucket(bucket_name)
    objects = bucket.object_versions
    if key_prefix:
        objects = objects.filter(Prefix=key_prefix)
    else:
        objects = objects.iterator()
        
    targets = []
    for o in objects:
        targets.append({
            'Key': o.object_key,
            'VersionId': o.version_id
        })
    
    bucket.delete_objects(Delete={
        'Objects': targets,
        'Quiet': True
    })
    
    return len(targets)


def delete_buckets(name=None):
    count = 0
    if name:
        bucket = get_bucket(name)
        if bucket:
            bucket.delete()
            bucket.wait_until_not_exists()
            count += 1
    else:
        count = 0
        client = boto3.resource('s3')
        for bucket in client.buckets.iterator():
            try:
                bucket.delete()
                bucket.wait_until_not_exists()
                count += 1
            except ClientError as err:
                log.warning(f'Bucket {bucket.name}: {err}')
    return count


    
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(
        title='Commands',
    )
    
    # Create Bucket
    sp_action_bucket_create = subparsers.add_parser(
        'create_bucket',
        help='Creates an S3 Bucket'
    )
    
    sp_action_bucket_create.add_argument(
        'bucket_name',
        help='The name of the bucket must be unique.'
    )
    
    sp_action_bucket_create.add_argument(
        '--region',
        '-R',
        help="""The region where the bucket is created. 
        Defaults to ap-southeast-1"""
    )
    
    sp_action_bucket_create.set_defaults(func=create_bucket)
    
    # List Buckets
    sp_action_bucket_list = subparsers.add_parser(
        'list_buckets',
        help='List all buckets.'
    )
    
    sp_action_bucket_list.set_defaults(func=list_buckets)
    
    # Get Bucket
    sp_action_bucket_get = subparsers.add_parser(
        'get_bucket',
        help='get existing bucket or create if bucket does not exist'
    )
    
    sp_action_bucket_get.add_argument(
        'bucket_name',
        help='Name of an existing bucket.'
    )
    
    sp_action_bucket_get.add_argument(
        '--create', '-C',
        action='store_true',
        help='create bucket if not exists'
    )
    
    sp_action_bucket_get.add_argument(
        '--region', '-R'
    )
    
    sp_action_bucket_get.set_defaults(func=get_bucket)
    
    # Create and Upload Bucket Object
    sp_action_bucket_object_create = subparsers.add_parser(
        'create_bucket_object',
        help='create and upload bucket object'
    )
    
    sp_action_bucket_object_create.add_argument(
        'bucket_name'
    )
    
    sp_action_bucket_object_create.add_argument(
        'file_path'
    )
    
    sp_action_bucket_object_create.add_argument(
        '--key-prefix', '-K'
    )
    
    sp_action_bucket_object_create.set_defaults(func=create_bucket_object)
    
    # Get Object from Bucket
    sp_action_bucket_object_get = subparsers.add_parser(
        'get_bucket_object',
        help='Get an object from a bucket.'
    )
    
    sp_action_bucket_object_get.add_argument(
        'bucket_name'
    )
    
    sp_action_bucket_object_get.add_argument(
        'object_key'
    )
    
    sp_action_bucket_object_get.add_argument(
        '--destination', '-D'
    )
    
    sp_action_bucket_object_get.add_argument(
        '--version-id', '-V'
    )
    
    sp_action_bucket_object_get.set_defaults(func=get_bucket_object)
    
    sp_action_bucket_object_list = subparsers.add_parser(
        'list_bucket_objects',
        help='List objects in a bucket.'
    )
    
    sp_action_bucket_object_list.add_argument(
        'bucket_name'
    )
    
    sp_action_bucket_object_list.add_argument(
        '--prefix', '-P'
    )
    
    sp_action_bucket_object_list.set_defaults(func=list_bucket_objects)
    
    # Enable versioning
    sp_action_bucket_enable_versioning = subparsers.add_parser(
        'enable_bucket_versioning',
        help='Enable bucket versioning.'
    )
    
    sp_action_bucket_enable_versioning.add_argument(
        'bucket_name'
    )
    
    sp_action_bucket_enable_versioning.set_defaults(func=enable_bucket_versioning)
    
    # Delete object
    sp_action_object_delete = subparsers.add_parser(
        'delete_bucket_object',
        help='delete a single object in a bucket.'
    )
    
    sp_action_object_delete.add_argument(
        'bucket_name'
    )
    
    sp_action_object_delete.add_argument(
        'object_key'
    )
    
    sp_action_object_delete.set_defaults(func=delete_bucket_object)
    
    sp_action_bucket_objects_delete = subparsers.add_parser(
        'delete_bucket_objects',
        help='delete many objects in a bucket.'
    )
    
    sp_action_bucket_objects_delete.add_argument(
        'bucket_name'
    )
    
    sp_action_bucket_objects_delete.add_argument(
        '--key-prefix', '-K'
    )
    
    sp_action_bucket_objects_delete.set_defaults(func=delete_bucket_objects)
    
    sp_action_delete_buckets = subparsers.add_parser(
        'delete_buckets',
        help='delete buckets in an account'
    )
    
    sp_action_delete_buckets.add_argument(
        '--bucket-name'
    )
    
    sp_action_delete_buckets.set_defaults(func=delete_buckets)
    
    
    pargs = parser.parse_args()
    action = pargs.func.__name__ if hasattr(pargs, 'func') else ''
    if action == 'create_bucket':
        pargs.func(pargs.bucket_name)
    elif action == 'list_buckets':
        pargs.func()
    elif action == 'get_bucket':
        print(pargs.func(pargs.bucket_name, pargs.create, pargs.region))
    elif action == 'create_bucket_object':
        print(pargs.func(pargs.bucket_name, pargs.file_path, pargs.key_prefix))
    elif action == 'get_bucket_object':
        print(
            pargs.func(
                pargs.bucket_name, 
                pargs.object_key,
                pargs.destination, 
                pargs.version_id
            )
        )
    elif action == 'list_bucket_objects':
        objs = pargs.func(pargs.bucket_name, pargs.prefix)
        for o in objs:
            print(o)
    elif action == 'enable_bucket_versioning':
        print((pargs.func(pargs.bucket_name)))
    elif action == 'delete_bucket_object':
        print(pargs.func(pargs.bucket_name, pargs.object_key))
    elif action == 'delete_bucket_objects':
        print(pargs.func(pargs.bucket_name, pargs.key_prefix))
    elif action == 'delete_buckets':
        print(pargs.func(pargs.bucket_name))
    else:
        print('Missing or Invalid Command')
        
    print('Done.')
