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
    str = str.strip()
    return re.sub(r'[\[\]\'"\(\)\{\}]', '', str)

def remove_space(list_data):
    cleaned_data = []
    for i in range(len(list_data)):
        list_data[i] = list_data[i].strip()
    return list_data

def match_join_type(join_type):
    if join_type == 'Join':
        join_type = 'inner'
    if join_type == "Outer":
        join_type = 'outer'
    if join_type =="Left outer":
        join_type = 'left_outer'
    if join_type == "Right outer":
        join_type = 'right_outer'
    return join_type    
    

def return_valid_df_name(df_name):
    return  re.sub("[^a-zA-Z0-9_]", "", df_name)   

def return_valid_exp(exp):
   exp = exp.strip()
   exp = re.sub('["]', "'",exp)
   return re.sub("[\n\r]", " ", exp) 

def check_column_is_valid(col):
    if re.findall("[^a-zA-Z0-9_]",col):
        col = "'"+col+"'"
    return col    


   