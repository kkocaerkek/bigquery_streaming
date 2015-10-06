#!/usr/bin/env python
# -*- coding: utf-8 -*-

import httplib2
from google.appengine.api import memcache
from oauth2client.appengine import AppAssertionCredentials
from apiclient.discovery import build
from apiclient.errors import HttpError
from google.appengine.api.app_identity import get_application_id
import logging
import json
import hashlib
import datetime


_bigquery_service = None


def get_big_query_service():
    global _bigquery_service
    if _bigquery_service is None:
        credentials = AppAssertionCredentials(scope='https://www.googleapis.com/auth/bigquery')
        http = credentials.authorize(httplib2.Http(memcache))
        _bigquery_service = build('bigquery', 'v2', http=http)
    return _bigquery_service


def list_datasets(project_id):
    bigquery_service = get_big_query_service()
    if project_id:
        try:
            r = bigquery_service.datasets().list(projectId=project_id).execute()
            return r
        except Exception, e:
            logging.error(e)
            raise e


def insert_dataset(project_id, dataset_id):
    bigquery_service = get_big_query_service()
    if project_id and dataset_id:
        try:
            datasetReference = {
                "datasetId": dataset_id,
                "projectId": project_id,
            }
            body = {
                "datasetReference": datasetReference
            }
            r = bigquery_service.datasets().insert(projectId=project_id, body=body).execute()
            return r
        except HttpError, e:
            logging.error(e)
            responsecode = e.resp.status
            if responsecode == 409:
                """ conflict """
                memcache.set(dataset_id, True)
                return
            # additional log or alarm
            raise e
        except Exception, e:
            logging.error(e)
            raise e


def get_dataset(project_id, dataset_id):
    bigquery_service = get_big_query_service()
    if project_id and dataset_id:
        try:
            r = bigquery_service.datasets()\
                .get(projectId=project_id, datasetId=dataset_id).execute()
            return r
        except Exception, e:
            logging.error(e)
            raise e


def list_tables(dataset_id, project_id=None):
    if project_id is None:
        project_id = get_application_id()
    bigquery_service = get_big_query_service()
    if project_id and dataset_id:
        try:
            r = bigquery_service.tables().list(projectId=project_id, datasetId=dataset_id).execute()
            return r
        except Exception, e:
            logging.error(e)
            raise e


def insert_table(project_id, dataset_id, table_id, fields):
    bigquery_service = get_big_query_service()
    if project_id and dataset_id and table_id and fields:
        try:
            tableReference = {
                "datasetId": dataset_id,
                "projectId": project_id,
                "tableId": table_id
            }
            body = {
                "tableReference": tableReference,
                "schema": fields
            }
            r = bigquery_service.tables()\
                .insert(projectId=project_id, datasetId=dataset_id, body=body).execute()
            set_last_schema_hash(dataset_id, table_id, fields)
            return r
        except HttpError, e:
            logging.error(e)
            responsecode = e.resp.status
            if responsecode == 409:
                """ conflict """
                return
            raise e
        except Exception, e:
            logging.error(e)
    else:
        return False


def update_table(project_id, dataset_id, table_id, fields):
    bigquery_service = get_big_query_service()
    if project_id and dataset_id and table_id and fields:
        try:
            body = {
                "schema": fields
            }
            request = bigquery_service.tables()\
                .update(projectId=project_id, datasetId=dataset_id, tableId=table_id, body=body)

            r = request.execute()
            set_last_schema_hash(dataset_id, table_id, fields)
            return r
        except HttpError, e:
            logging.error(e)
            raise e
        except Exception, e:
            logging.error(e)
            raise e


def set_last_schema_hash(dataset_id, table_id, schema):
    if isinstance(schema, dict):
        schema = json.dumps(schema)
    hash_value = hashlib.md5(schema).hexdigest()
    memcache.set("schema_%s_%s" % (dataset_id, table_id), hash_value)


def get_last_schema_hash(dataset_id, table_id):
    hash_value = memcache.get("schema_%s_%s" % (dataset_id, table_id))
    return hash_value


def create_table_if_not_exists(dataset_id, table_id, table_schema, project_id=None):
    if project_id is None:
        project_id = get_application_id()

    if memcache.get(dataset_id + table_id):
        """ cache says table exists """
        return

    table_list = list_tables(dataset_id)
    for table in table_list.get("tables", []):
        if table["tableReference"]["tableId"] == table_id:
            """ table exists. put it to cache"""
            memcache.set(dataset_id + table_id, True)
            return
    """ table not exists, create it """
    insert_success = insert_table(project_id, dataset_id, table_id, table_schema)
    if insert_success:
        memcache.set(dataset_id + table_id, True)


def update_table_if_outdated(dataset_id, table_id, table_schema, project_id=None):
    if project_id is None:
        project_id = get_application_id()

    last_hash = get_last_schema_hash(dataset_id, table_id)
    current_hash = hashlib.md5(json.dumps(table_schema)).hexdigest()
    if last_hash != current_hash:
        """ table schema is outdated """
        logging.info("%s:%s needs to be updated" % (dataset_id, table_id))
        update_table(project_id, dataset_id, table_id, table_schema)


def insert_rows(dataset_id, table_id, body, project_id=None):
    if project_id is None:
        project_id = get_application_id()
    bigquery_service = get_big_query_service()
    if project_id and dataset_id and table_id and body:
        try:
            r = bigquery_service.tabledata().insertAll(projectId=project_id,
                                                       datasetId=dataset_id, tableId=table_id,
                                                       body=body).execute()
            return r
        except Exception, e:
            logging.error(e)
            raise e


def create_dataset_if_not_exists(target_dataset_id, project_id=None):
    if project_id is None:
        project_id = get_application_id()

    dataset_cache_key = "dataset_%s" % target_dataset_id
    cached_dataset = memcache.get(dataset_cache_key)
    logging.debug("cached dataset: %s" % str(cached_dataset))
    if cached_dataset is not None:
        logging.debug("dataset: %s found in cache" % cached_dataset)
        return cached_dataset

    dataset_list_response = list_datasets(project_id)
    """ if project does not have any datasets yet, returns response without 'datasets' field"""
    if "datasets" not in dataset_list_response:
        dataset_list_response["datasets"] = []
    for dataset in dataset_list_response["datasets"]:
        if dataset["datasetReference"]["datasetId"] == target_dataset_id:
            logging.debug("dataset: %s found in dataset list" % target_dataset_id)
            memcache.set(dataset_cache_key, target_dataset_id)
            return target_dataset_id

    logging.debug("dataset: %s inserting" % target_dataset_id)
    insert_dataset(project_id, target_dataset_id)
    logging.debug("dataset: %s inserted" % target_dataset_id)
    memcache.set(dataset_cache_key, target_dataset_id)
    return cached_dataset


def get_timestamp_value(datetime_value):
    if not isinstance(datetime_value, datetime.datetime):
        return None
    if datetime_value.year < 1900:
        return None
    return datetime_value.strftime("%Y-%m-%d %H:%M:%S")