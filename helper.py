import re
from constants import *
def compare_datatype(schema):
    datatype_match = {'numeric': [], 'string': [], 'timestamp': [], 'boolean': []}
    for i in schema:
        if schema[i].lower() in numeric_type:
            datatype_match['numeric'].append(i)
        elif schema[i].lower() in string_type:
            datatype_match['string'].append(i)
        elif schema[i].lower() in timestamp_type:
            datatype_match['timestamp'].append(i)
        elif schema[i].lower() in boolean_type:
            datatype_match['boolean'].append(i)
        else:
            datatype_match['string'].append(i)
    return datatype_match

def cleanse_data(str):
    return re.sub(r'[\[\]\'\"\(\)\{\}]', '', str)

def remove_space(list_data):
    cleaned_data = []
    for i in range(len(list_data)):
        list_data[i] = list_data[i].strip()
    return list_data