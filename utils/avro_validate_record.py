#!/usr/bin/env python

'''
Given an Avro schema and a record, determine if the record is compliant against
the schema
'''

import pickle
import simplejson as json
import sys

from tornado_producer import avro_serializer

schema_registry_url = 'http://10.42.32.102:8081'  # dev schema server
# schema_registry_url = 'http://10.42.45.251:8081'  # prod schema server

# A map of Python types to compatible Avro types
type_map = {
    bool: 'boolean',
    int: 'long',
    float: 'double',
    str: 'string',
    unicode: 'string',
    list: 'array',
}


def validate(subject, record_str):
    # Load schema
    schema = avro_serializer.AvroMessageSerializer(subject, schema_registry_url)

    # Load record
    record_str = record_str.rstrip().replace('\\n', '').replace("\\'", '').replace("\\t", '')
    # First try JSON
    try:
        record = json.loads(record_str)
    # If that fails attempt to pickle
    except ValueError:
        record = pickle.loads(record_str)

    result = _check_record(record, schema.avro_schema['fields'])
    print
    for error in result:
        print error + ': ' + str(result[error])


def _check_record(record, schema):
    result = {
        'Missing': [],
        'Mismatched Type': [],
        'Nested (not checked)': []
    }

    for field in schema:
        if not field['name'] in record:
            #print 'WARN: ' + field['name'] + ' missing'
            result['Missing'].append(field['name'])
            continue

        if type(record[field['name']]) == dict:
            # print 'WARN: Skipping nested object ' + field['name']
            result['Nested (not checked)'].append(field['name'])
            continue

        if type_map[type(record[field['name']])] != _get_type(field['type']):
            result['Mismatched Type'].append(field['name'])
            print 'ERROR: {} has mismatched types {}, {}, record value: {}'.format(field['name'], type_map[type(record[field['name']])], _get_type(field['type']), record[field['name']])

    return result


def _get_type(field_type):
    # If string is the type itself so return
    if type(field_type) in [str, unicode]:
        return field_type
    # Search list for non-null type
    elif 'null' in field_type:
        field_type.remove('null')

    # Array types have a different format
    if type(field_type[0]) == dict:
        return field_type[0]['type']
    elif len(field_type) == 1:
        return field_type[0]
    else:
        return field_type


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print '''
Usage:
    ./scripts/avro_validate_record.py <schema-subject> <record-filename>

Where:
    schema-subject is the name of a schema subject
    record-filename is a file containing a JSON or Python pickled
        dictionary to validate against schema-subject

E.g.
    Dump a Python dictionary with the following line:
        import pickle; del bid_request['rejected']; pickle.dump(bid_request, open('bid_request.data', 'wb'))

    Run this script:
        ./scripts/avro_validate_record.py inapp-bid-request-value bid_request.data
'''
        sys.exit(1)

    validate(sys.argv[1], open(sys.argv[2]).read())
