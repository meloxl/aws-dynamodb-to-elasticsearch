import json
import boto3
import boto3.dynamodb.types
import logging
import argparse
from boto3 import Session
from ddb_to_es import handler
import os
from multiprocessing import Pool

logging.basicConfig()

reports = []
object_amount = 0
partSize = 0


def main():
    parser = argparse.ArgumentParser(description='Set-up importing to dynamodb')
    parser.add_argument('--rn', metavar='R', help='AWS region')
    parser.add_argument('--tn', metavar='T', help='table name')
    parser.add_argument('--ak', metavar='AK', help='aws access key')
    parser.add_argument('--sk', metavar='AS', help='aws secret key')
    parser.add_argument('--esarn', metavar='ESARN', help='event source ARN')
    parser.add_argument('--lf', metavar='LF', help='lambda function that posts data to es')
    parser.add_argument('--es', metavar='ES', help='es host url without http/https')
    parser.add_argument('--index', metavar='INDEX', help='es index name')
    parser.add_argument('--id', metavar='ID', help='es id name')


    scan_limit = 200
    args = parser.parse_args()

    if (args.rn is None):
        print('Specify region parameter (-rn)')
        return

    import_dynamodb_items_to_es(args.tn, args.sk, args.ak, args.rn,
                                args.esarn, args.lf, scan_limit, args.es, args.index, args.id)


def import_dynamodb_items_to_es(table_name, aws_secret, aws_access, aws_region, event_source_arn, lambda_f, scan_limit, es_host, es_index, es_id):
    global reports
    global partSize
    global object_amount

    logger = logging.getLogger()
    logger.setLevel(logging.ERROR)

    session = Session(aws_access_key_id=aws_access,
                      aws_secret_access_key=aws_secret, region_name=aws_region)

    os.environ['AWS_ACCESS_KEY_ID'] = aws_access
    os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret
    os.environ['AWS_REGION'] = aws_region
    os.environ['ES_HOST'] = es_host
    os.environ['ES_INDEX'] = es_index
    os.environ['ES_ID'] = es_id

    dynamodb = session.resource('dynamodb')
    logger.info('dynamodb: %s', dynamodb)
    ddb_table_name = table_name
    table = dynamodb.Table(ddb_table_name)
    logger.info('table: %s', table)
    ddb_keys_name = [a['AttributeName'] for a in table.attribute_definitions]
    logger.info('ddb_keys_name: %s', ddb_keys_name)
    response = None

    pool = Pool(5)

    while True:
        if not response:
            response = table.scan(Limit=scan_limit)
        else:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], Limit=scan_limit)
        for i in response["Items"]:
            ddb_keys = {k: i[k] for k in i if k in ddb_keys_name}
            ddb_data = boto3.dynamodb.types.TypeSerializer().serialize(i)["M"]
            ddb_keys = boto3.dynamodb.types.TypeSerializer().serialize(ddb_keys)["M"]
            record = {
                "dynamodb": {"SequenceNumber": "0000", "Keys": ddb_keys, "NewImage": ddb_data},
                "awsRegion": aws_region,
                "eventName": "INSERT",
                "eventSourceARN": event_source_arn,
                "eventSource": "aws:dynamodb"
            }
            partSize += 1
            object_amount += 1
            logger.info(object_amount)
            reports.append(record)
            # print("reports: %s" % reports)

            if partSize >= 100:
                pool.apply_async(send_to_eslambda, args=(reports, lambda_f))
            # send_to_eslambda(reports, lambda_f)

        if 'LastEvaluatedKey' not in response:
            break

    if partSize > 0:
        send_to_eslambda(reports, lambda_f)

    pool.close()
    pool.join()


def send_to_eslambda(items, lambda_f):
    global reports
    global partSize
    records_data = {
        "Records": items
    }
    records = json.dumps(records_data)
    print("records_data: %s" % records_data)
    handler(records_data, '')


if __name__ == "__main__":
    main()
