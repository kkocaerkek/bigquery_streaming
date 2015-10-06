#!/usr/bin/env python
# -*- coding: utf-8 -*-
from bigquery import  insert_rows, create_table_if_not_exists,\
    update_table_if_outdated, get_timestamp_value, create_dataset_if_not_exists


REALTIME_TEST_TABLE_ID = "SIMPLE_CALL_HISTORY"
REALTIME_TEST_TABLE_SCHEMA = {
    "fields": [
        {"name": "key", "type": "STRING"},
        {"name": "calldate", "type": "TIMESTAMP"},
        {"name": "answered", "type": "BOOLEAN"},
        {"name": "duration", "type": "INTEGER"},
        {"name": "callerid", "type": "STRING"},
        {"name": "called_number", "type": "STRING"}
    ]
}


def streaming_insert_call_history(history_entity):
    dataset_id = "REALTIME_EXAMPLE"
    create_dataset_if_not_exists(dataset_id)
    create_table_if_not_exists(dataset_id, REALTIME_TEST_TABLE_ID, REALTIME_TEST_TABLE_SCHEMA)
    update_table_if_outdated(dataset_id, REALTIME_TEST_TABLE_ID, REALTIME_TEST_TABLE_SCHEMA)

    record_body = {
        "key": unicode(history_entity.key()),
        "calldate": get_timestamp_value(history_entity.calldate),
        "answered": history_entity.answered,
        "duration": history_entity.duration,
        "callerid": history_entity.callerid,
        "called_number": history_entity.called_number
    }

    rows = {
        "rows": [
            {
                "json": record_body,
                "insertId": unicode(history_entity.key())
            }
        ]
    }

    insert_rows(dataset_id, REALTIME_TEST_TABLE_ID, rows)