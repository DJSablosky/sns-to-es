# coding: utf-8
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

region = 'us-east-1'  # e.g. us-west-1
service = 'es'

hosts = ["elastic-1.ardentmc.com", "elastic-2.ardentmc.com",
         "elastic-3.ardentmc.com"]

headers = {"Content-Type": "application/json"}

s3 = boto3.client('s3')

session = boto3.Session(profile_name='corpnew')
