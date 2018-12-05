#!/usr/bin/python
# -*- coding:  utf-8 -*-

""" CloudTrail Analytics

This module is based on Expedia reInvent method for reading SNS messages
to get the S3 file location for CloudTrail logs and store them in
Elasticsearch.
"""
from __future__ import print_function
import json
import boto3
import logging
import datetime
import gzip
import urllib
import os
import traceback
import io
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection
import boto3


region = 'us-east-1'  # e.g. us-west-1
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key,
                   region, service, session_token=credentials.token)

hosts = ["elastic-1.ardentmc.com", "elastic-2.ardentmc.com",
         "elastic-3.ardentmc.com"]

headers = {"Content-Type": "application/json"}

s3 = boto3.client('s3')

es = Elasticsearch(
    hosts=hosts,
    http_auth=awsauth,
    use_ssl=False,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)


# Lambda execution starts here
def handler(event, context):
    print(es.info())
    logger = logging.getLogger(__name__)
    logger.info('Event:  ' + json.dumps(event, indent=2))
    print(json.dumps(event, indent=2))

    try:
        s3Bucket = json.loads(event['Records'][0]['Sns']['Message'])['s3Bucket']
        print("s3 Bucket is",  s3Bucket)
        s3ObjectKey = urllib.parse.unquote_plus(json.loads(event['Records'][0]['Sns']['Message'])['s3ObjectKey'][0])
        print("s3ObjectKey is ", s3ObjectKey)
        logger.info('S3 Bucket: ' + s3Bucket)
        logger.info('S3 Object Key: ' + s3ObjectKey)
        response = s3.get_object(Bucket=s3Bucket, Key=s3ObjectKey)

        # Read compressed data from log.gz file
        buffer = io.BytesIO(response['Body'].read())
        content = gzip.GzipFile(None, 'rb', fileobj=buffer).read().decode('utf-8')

        for record in json.loads(content)['Records']:
            recordJson = json.dumps(record)
            logger.info(recordJson)
            indexName = 'ct-' + datetime.datetime.now().strftime("%Y-%m-%d")
            print(indexName)

            if not es.indices.exists(indexName):
                # since we are running locally, use one shard and no replicas
                request_body = {
                    "settings":  {
                        "number_of_shards":  7,
                        "number_of_replicas":  2
                    }
                }
                print("creating '%s' index..." % (indexName))
                index_res = es.indices.create(index=indexName, body=request_body)
                print(" response:  '%s'" & (index_res))

            res = es.index(index=indexName, doc_type='record', id=record['eventID'], body=recordJson)
            logger.info(res)
        return True
    except Exception as e:
        logger.error('Something went wrong: ' + str(e))
        traceback.print_exc()
        return False
