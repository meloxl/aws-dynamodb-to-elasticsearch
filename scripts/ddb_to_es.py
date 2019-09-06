from __future__ import print_function

import json
import re
import boto3
import os
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from requests_aws4auth import AWS4Auth

reserved_fields = ["uid", "_id", "_type", "_source", "_all", "_parent",
                   "_fieldnames", "_routing", "_index", "_size", "_timestamp", "_ttl"]


# Process DynamoDB Stream records and insert the object in ElasticSearch
# Use the Table name as index and doc_type name
# Force index refresh upon all actions for close to realtime reindexing
# Use IAM Role for authentication
# Properly unmarshal DynamoDB JSON types. Binary NOT tested.

def handler(event, context):
    session = boto3.session.Session()
    credentials = session.get_credentials()

    # Get proper credentials for ES auth
    awsauth = AWS4Auth(os.getenv('AWS_ACCESS_KEY_ID'), os.getenv('AWS_SECRET_ACCESS_KEY'),
                       os.getenv('AWS_REGION', 'us-east-1'), 'es', session_token=os.getenv('AWS_SESSION_TOKEN'))
    host = os.getenv('ES_HOST', "")
    # Connect to ES
    es = Elasticsearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    bulks = {}

    # print("Cluster info:")
    # print(es.info())

    # Loop over the DynamoDB Stream records
    for record in event['Records']:

        try:
            if record['eventName'] == "INSERT":
                insert_document(es, record, bulks)
            elif record['eventName'] == "REMOVE":
                remove_document(record, bulks)
            elif record['eventName'] == "MODIFY":
                modify_document(record, bulks)

        except Exception as e:
            print("Failed to process:")
            print(json.dumps(record))
            print("Error:")
            print(e)
            continue

    es_bulk(es, bulks)

# Process MODIFY events


def modify_document(record, bulks):
    # table = getTable(record)
    # print("Dynamo Table: " + table)

    docId = generateId(record)
    # print("KEY")
    # print(docId)

    # Unmarshal the DynamoDB JSON to a normal JSON
    doc = unmarshalJson(record['dynamodb']['NewImage'])
    doc['_op_type'] = 'update'

    bulks[docId] = doc

    # print("Updated document:")
    # print(doc)

    # We reindex the whole document as ES accepts partial docs
    # es.index(index=table,
    #          body=doc,
    #          id=docId,
    #          doc_type=os.getenv('ES_TYPE', 'doc'),
    #          refresh=True)
    #
    # print("Successly modified - Index: " + table + " - Document ID: " + docId)

# Process REMOVE events


def remove_document(record, bulks):
    # table = getTable()
    # print("Dynamo Table: " + table)

    docId = generateId(record)
    doc = {}
    doc['_op_type'] = 'delete'
    # print("Deleting document ID: " + docId)

    bulks[docId] = doc

    # es.delete(index=table,
    #           id=docId,
    #           doc_type=os.getenv('ES_TYPE', 'doc'),
    #           refresh=True)
    #
    # print("Successly removed - Index: " + table + " - Document ID: " + docId)

# Process INSERT events


def insert_document(es, record, bulks):
    table = getTable()
    # print("Dynamo Table: " + table)

    # Create index if missing
    if es.indices.exists(table) == False:
        print("Create missing index: " + table)

        es.indices.create(table,
                          body='{"settings": { "index.mapping.coerce": true } }')

        # print("Index created: " + table)

    # Unmarshal the DynamoDB JSON to a normal JSON
    doc = unmarshalJson(record['dynamodb']['NewImage'])

    doc['_op_type'] = 'create'

    # print("New document to Index:")
    # print(doc)

    newId = generateId(record)

    bulks[newId] = doc

    # es.index(index=table,
    #          body=doc,
    #          id=newId,
    #          doc_type=os.getenv('ES_TYPE', 'doc'),
    #          refresh=True)
    #
    # print("Successly inserted - Index: " + table + " - Document ID: " + newId)


def gendata(bulks):
    table = getTable()

    for idx, doc in bulks.items():
        op_type = doc.pop('_op_type')
        if op_type == 'create':
            yield {
                "_index": table,
                "_type": os.getenv('ES_TYPE', 'doc'),
                "_source": doc,
                "_id": idx,
                "_op_type": 'index'
            }
        elif op_type == 'update':
            yield {
                "_index": table,
                "_type": os.getenv('ES_TYPE', 'doc'),
                "_source": doc,
                "_id": idx,
                "_op_type": 'index'
            }
        elif op_type == 'delete':
            yield {
                "_index": table,
                "_type": os.getenv('ES_TYPE', 'doc'),
                "_id": idx,
                "_op_type": 'delete'
            }
        else:
            print("op_type not support!")


def es_bulk(es, bulks):

    helpers.bulk(es, gendata(bulks), max_retries=0, raise_on_error=False)

    print("Successly bulk : " + str(len(bulks)) + " records")

# Return the dynamoDB table that received the event. Lower case it


def getTable():

    return os.getenv('ES_INDEX', '')

# Generate the ID for ES. Used for deleting or updating item later


def generateId(record):
    # 默认排序见和分区键的情况
    es_id = os.getenv('ES_ID', '')
    keys = unmarshalJson(record['dynamodb']['Keys'])
    if  es_id != "":
        es_id_list = eval(es_id)
        key = '|'.join(es_id_list)
        # print(type(newId))
        newId = keys[key]
        # print(newId)
        return newId

    # Concat HASH and RANGE key with | in between
    newId = ""
    i = 0
    for key, value in keys.items():
        if (i > 0):
            newId += "|"
        newId += str(value)
        i += 1

    print(newId)
    return newId

# Unmarshal a JSON that is DynamoDB formatted


def unmarshalJson(node):
    data = {}
    data["M"] = node
    return unmarshalValue(data, True)

# ForceNum will force float or Integer to


def unmarshalValue(node, forceNum=False):
    for key, value in node.items():
        if (key == "NULL"):
            return None
        if (key == "S" or key == "BOOL"):
            return value
        if (key == "N"):
            if (forceNum):
                return int_or_float(value)
            return value
        if (key == "M"):
            data = {}
            for key1, value1 in value.items():
                if key1 in reserved_fields:
                    key1 = key1.replace("_", "__", 1)
                data[key1] = unmarshalValue(value1, True)
            return data
        if (key == "BS" or key == "L"):
            data = []
            for item in value:
                data.append(unmarshalValue(item))
            return data
        if (key == "SS"):
            data = []
            for item in value:
                data.append(item)
            return data
        if (key == "NS"):
            data = []
            for item in value:
                if (forceNum):
                    data.append(int_or_float(item))
                else:
                    data.append(item)
            return data

# Detect number type and return the correct one


def int_or_float(s):
    try:
        return int(s)
    except ValueError:
        return float(s)


if __name__ == "__main__":
    handler(event, "")
