import os
import uuid
import json
import argparse

import boto3


TABLES = {
    'xray': {
        'prefix': 'xray-test',
        'env_var': 'XRAY_DYNAMO_TABLE',
        'date_key': 'date'
    },
}


def create_table(table_config):
    table_name = '%s-%s' % (table_config['prefix'], str(uuid.uuid4()))
    client = boto3.client('dynamodb')
    key_schema = [
        {
            'AttributeName': table_config['date_key'],
            'KeyType': 'HASH',
        }
    ]
    attribute_definitions = [
        {
            'AttributeName': table_config['date_key'],
            'AttributeType': 'S',
        }
    ]
    client.create_table(
        TableName=table_name,
        KeySchema=key_schema,
        AttributeDefinitions=attribute_definitions,
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5,
        }
    )
    waiter = client.get_waiter('table_exists')
    waiter.wait(TableName=table_name, WaiterConfig={'Delay': 1})
    return table_name
    
def create_sns_topic():
    sns_client = boto3.client('sns', "ap-southeast-2")
    sns_topic_arn = sns_client.create_topic( Name = "x-ray-topic")['TopicArn']  
    return sns_topic_arn   


def record_as_env_var(key, value, stage:None):

    with open(os.path.join('.chalice', 'config.json')) as f:
        data = json.load(f)
        data['stages'].setdefault(stage, {}).setdefault(
            'environment_variables', {}
        )[key] = value
    with open(os.path.join('.chalice', 'config.json'), 'w') as f:
        serialized = json.dumps(data, indent=2, separators=(',', ': '))
        f.write(serialized + '\n')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--stage', default='dev')
    parser.add_argument('-t', '--table-type', default='xray',
                        choices=['xray'],
                        help='Specify which type to create')
    args = parser.parse_args()
    table_config = TABLES[args.table_type]
    table_name = create_table(
        table_config
    )
    record_as_env_var(table_config['env_var'], table_name, args.stage)

if __name__ == '__main__':
    main()
