import json
import sys
import logging

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
import operator as op


logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s')
log = logging.getLogger()


def parse_tabledef(conf_file):
    require_keys = [
        'table_name',
        'pk',
        'pkdef',
    ]
    with open(conf_file) as fh:
        conf = json.loads(fh.read())
        if require_keys == list(conf.keys()):
            return conf
        else:
            raise KeyError('Invalid configuration.')


def create_dynamo_table(table_name, pk, pkdef):
    ddb = boto3.resource('dynamodb')
    table = ddb.create_table(
        TableName=table_name,
        KeySchema=pk,
        AttributeDefinitions=pkdef,
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    
    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
    
    return table


def get_dynamo_table(table_name):
    ddb = boto3.resource('dynamodb')
    return ddb.Table(table_name)


def create_product(category, sku, **item):
    table = get_dynamo_table('products')
    keys = {
        'category': category,
        'sku': sku,
    }
    item.update(keys)
    table.put_item(Item=item)
    return table.get_item(Key=keys)['Item']


def update_product(category, sku, **item):
    table = get_dynamo_table('products')
    keys = {
        'category': category,
        'sku': sku,
    }
    expr = ', '.join([f'{k}=:{k}' for k in item.keys()])
    vals = {f':{k}' : v for k, v in item.items()}
    table.update_item(
        Key=keys,
        UpdateExpression=f'SET {expr}',
        ExpressionAttributeValues=vals,
    )
    return table.get_item(Key=keys)['Item']


def delete_product(category, sku):
    table = get_dynamo_table('products')
    keys = {
        'category': category,
        'sku': sku,
    }
    res = table.delete_item(Key=keys)
    if res.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200:
        return True
    else:
        log.error(f'There was an error when deleting the product: {res}')
        return False


def create_dynamo_items(table_name, items, keys=None):
    table = get_dynamo_table(table_name)
    params = {
        'overwrite_by_keys': keys,
    } if keys else {}
    with table.batch_writer(**params) as batch:
        for item in items:
            batch.put_item(Item=item)
    return True


def query_products(key_expr, filter_expr=None):
    table = get_dynamo_table('products')
    params = {
        'KeyConditionExpression': key_expr,
    }
    if filter_expr:
        params['FilterExpression'] = filter_expr
    res = table.query(**params)
    return res['Items']


def scan_products(filter_expr):
    table = get_dynamo_table('products')
    params = {
        'FilterExpression': filter_expr,
    }
    res = table.scan(**params)
    return res['Items']


def delete_dynamo_table(table_name):
    table = get_dynamo_table(table_name)
    table.delete()
    table.wait_until_not_exists()
    return True


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers(
        title='DynamoDb Management Commands'
    )
    
    sp_create_dynamo_table = subparser.add_parser(
        'create-dynamo-table',
        help='Creates a DynamoDb Table.'
    )
    
    sp_create_dynamo_table.add_argument(
        'conf_file',
        help='The Table Definition File(JSON)'
    )
    
    sp_create_dynamo_table.set_defaults(func=create_dynamo_table)
    
    sp_get_dynamo_table = subparser.add_parser(
        'get-dynamo-table'
    )
    
    sp_get_dynamo_table.add_argument(
        'table_name'
    )
    
    sp_get_dynamo_table.set_defaults(func=get_dynamo_table)
    
    sp_delete_dynamo_table = subparser.add_parser(
        'delete-dynamo-table',
        help='Delete a DynamoDb Table. Accepts only one argument.'
    )
    
    sp_delete_dynamo_table.add_argument(
        'table_name',
        help='Table to Delete.'
    )
    
    sp_delete_dynamo_table.set_defaults(func=delete_dynamo_table)
    
    sp_create_product = subparser.add_parser(
        'create-product',
        help='Create a new Product Entry'
    )
    
    sp_create_product.add_argument(
        'category',
        help='Product Category.'
    )
    
    sp_create_product.add_argument(
        'sku',
        help='Product SKU.'
    )
    
    sp_create_product.add_argument(
        'item',
        help='Item Entry (in JSON)'
    )
    
    sp_create_product.set_defaults(func=create_product)
    
    sp_update_product = subparser.add_parser(
        'update-product',
        help='Update an existing product entry.'
    )
    
    sp_update_product.add_argument(
        'category',
        help='Product Category.'
    )
    
    sp_update_product.add_argument(
        'sku',
        help='Product SKU.'
    )
    
    sp_update_product.add_argument(
        'item',
        help='Item attributes to update.'
    )
    
    sp_update_product.set_defaults(func=update_product)
    
    sp_delete_product = subparser.add_parser(
        'delete-product',
        help='Delete a product entry.'
    )
    
    sp_delete_product.add_argument(
        'category'
    )
    
    sp_delete_product.add_argument(
        'sku'
    )
    
    sp_delete_product.set_defaults(func=delete_product)
    
    sp_create_dynamo_items = subparser.add_parser(
        'create-dynamo-items'
    )
    
    sp_create_dynamo_items.add_argument('table_name')
    
    sp_create_dynamo_items.add_argument('items')
    
    sp_create_dynamo_items.add_argument('--keys')
    
    sp_create_dynamo_items.set_defaults(func=create_dynamo_items)
    
    sp_query_products = subparser.add_parser('query-products')
    
    sp_scan_products = subparser.add_parser('scan-products')
    
    pargs = parser.parse_args()
    action = pargs.func.__name__ if hasattr(pargs, 'func') else ''
    
    if action == 'create_dynamo_table':
        conf = parse_tabledef(pargs.conf_file)
        pargs.func(**conf)
        sys.exit(0)
    elif action == 'get_dynamo_table':
        print(pargs.func(pargs.table_name))
        sys.exit(0)
    elif action == 'delete_dynamo_table':
        print(pargs.func(pargs.table_name))
        sys.exit(0)
    elif action == 'create_product':
        parsed_item = json.loads(pargs.item)
        print(pargs.func(pargs.category, pargs.sku, **parsed_item))
        sys.exit(0)
    elif action == 'update_product':
        parsed_item = json.loads(pargs.item)
        print(pargs.func(pargs.category, pargs.sku, **parsed_item))
        sys.exit(0)
    elif action == 'delete_product':
        print(pargs.func(pargs.category, pargs.sku))
        sys.exit(0)
    elif action == 'create_dynamo_items':
        print(pargs.func(pargs.table_name, pargs.items, pargs.keys))
        sys.exit(0)
    else:
        print('Invalid or Missing Command.')
        sys.exit(1)
