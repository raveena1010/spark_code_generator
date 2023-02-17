import root_node
import re
from helper import *
from constants import *
from builder import *


class Node_Operation:

    def __init__(self, data, code_file, pn_obj):
        self.workflow_data = data
        self.code_file = code_file
        self.pn_obj = pn_obj
        self.is_datasource_inhand = False
        self.datasource_dict = {}
        self.dataframe_name = {}
        self.read_data_columns = {}
        self.cached_df_schema = {}
        self.operation_ids =[]
        generate_dataframe_name(self)
        generate_datasource_dict(self)
        self.read_count = 1


    def get_schema_details_from_user(self,datasource_name):
        cols = input("Give Column names of {0} data ".format(datasource_name))
        cols = cleanse_data(cols)
        columns = cols.split(',')
        columns = remove_space(columns)
        data_types = input("Give data type for these columns as numeric/string/boolean/timestamp in order ").strip()
        data_types = cleanse_data(data_types)
        data_types = data_types.split(',')
        data_types = remove_space(data_types)
        schema = {columns[i]: data_types[i] for i in range(len(columns))}
        self.cached_df_schema[datasource_name] = schema
        return columns

    def read_dataframe(self, node):
        datasource_id = node['parameters']['data source']
        node_id = node['id']

        if self.is_datasource_inhand:
            datasource = self.datasource_dict[datasource_id]
            data_from = datasource['params']['datasourceType'] + "Params"
            file_format = datasource['params'][data_from]['fileFormat']
            datasource_name = datasource['params']['name']
            if datasource_name not in self.dataframe_name:
                if 'library' in data_from:
                    url = 'loc'
                    #url = input("Give file location of {0} data ".format(datasource_name)).strip()
                    #url = cleanse_data(url)
                else:
                    url = '"' + datasource['params'][data_from]['url'] + '"'   
                columns = self.get_schema_details_from_user(datasource_name)
                if file_format == "csv":
                    include_header = datasource['params'][data_from]['csvFileFormatParams']['includeHeader']
                    separator_type = separator[datasource['params'][data_from]['csvFileFormatParams']['separatorType']]
                    if not separator_type:
                        separator_type = datasource['params'][data_from]['csvFileFormatParams']['customSeparator']
                    code = f"{datasource_name} = spark.read.option('header',{include_header}).option('delimiter','{separator_type}').option('inferSchema',True).csv('{url}')"
                else:
                    code = f"{datasource_name} = spark.read.{file_format}('{url}')"

        else:
            datasource_name = input("Give Name for data_{0} ".format(self.read_count)).strip()
            df_name = cleanse_data(datasource_name)
            url = input("Give file location of data_{0} ".format(self.read_count)).strip()
            url = cleanse_data(url)
            columns = self.get_schema_details_from_user()
            file_format = input("Give file_format of data_{0} ".format(self.read_count)).strip()
            file_format = cleanse_data(file_format)
            if file_format == 'csv':
                include_header = input("Include header(True/False) for data_{0} ".format(self.read_count)).strip()
                separator_type = input("Separator type for data_{0} ".format(self.read_count)).strip()
                include_header = cleanse_data(include_header)
                separator_type = cleanse_data(separator_type)
                code = f"{datasource_name} = spark.read.option('header',{include_header}).option('delimiter','{separator_type}').option('inferSchema',True).csv('{url}')"
            else:
                code = f"{datasource_name} = spark.read.{file_format}('{url}')"
            self.read_count = self.read_count + 1

        self.read_data_columns[datasource_name] = columns
        self.dataframe_name[node['id']] = datasource_name
        self.code_file.write(code + '\n')
        set_df_name_for_child(self,node_id, datasource_name)
        add_child_in_output(self,node_id,datasource_name)


    def write_dataframe(self, node):
        node_id = node['id']
        datasource_id = node['parameters']['data source']
        df_name = self.dataframe_name[node_id]
        set_df_name_for_child(self,node_id, df_name) # not needed *****
        if self.is_datasource_inhand:
            datasource = self.datasource_dict[datasource_id]
            data_from = datasource['params']['datasourceType'] + "Params"
            file_format = datasource['params'][data_from]['fileFormat']
            datasource_name = datasource['params']['name']
            url = '"' + datasource['params'][data_from]['url'] + '"'
            if 'library' in url:
                url = input("Give file location of {0} data ".format(datasource_name)).strip()
                url = cleanse_data(url)
            code = f"{df_name}.write.{file_format}.path('{url}')"

        else:
            url = input(f"Give file location to save {df_name}").strip()
            url = cleanse_data(url)
            file_format = input(f"Give file_format of {df_name} ").strip()
            file_format = cleanse_data(file_format)
            if file_format == 'csv':
                include_header = input(f"Include header(True/False) for {df_name} ").strip()
                separator_type = input(f"Separator type for {df_name} ").strip()
                include_header = cleanse_data(include_header)
                separator_type = cleanse_data(separator_type)
                code = f"{df_name} = spark.read.option('header',{include_header}).option('delimiter','{separator_type}').option('inferschema',True).csv('{url}')"
            else:
                code = f"{df_name} = spark.read.{file_format}('{url}')"
        self.code_file.write(code + '\n')
        add_child_in_output(self,node_id,df_name) #not needed

    def filter_rows(self, node):
        node_id = node['id']
        df_name = self.dataframe_name[node_id]
        set_df_name_for_child(self,node_id,df_name)
        condition = node["parameters"]['condition']
        condition = condition.replace("'",'"')
        code = f"{df_name} = {df_name}.filter('{condition}')"
        self.code_file.write(code + '\n')
        #get the parent of this and update that as schema as no change in cols
        parents_id = self.pn_obj.child_parent[node_id]
        parent_name = self.dataframe_name[parents_id[0]]
        schema = self.cached_df_schema[parent_name]
        self.cached_df_schema[df_name] = schema
        add_child_in_output(self,node_id,self.dataframe_name[node_id])


    def filter_columns(self, node):
        node_id = node['id']
        df_name = self.dataframe_name[node_id]
        set_df_name_for_child(self,node_id,df_name)
        parents_id = self.pn_obj.child_parent[node_id]
        parent_name = self.dataframe_name[parents_id[0]]
        schema = self.cached_df_schema[parent_name]
        selections = node['parameters']["selected columns"]['selections']
        is_exclude = node['parameters']["selected columns"]['excluding']
        filterby_range = False
        index_range = []
        required_cols = []
        for ele in selections:
            selection_type = ele['type']
            values = ele['values']
            if selection_type == 'indexRange':
                filterby_range = True
                index_range.append(values)
            if selection_type == 'typeList':
                cols_to_add = self.filter_col_bytype(df_name,values,is_exclude,schema)
                required_cols.extend(cols_to_add)
            if selection_type == 'columnList':
                cols_to_add = self.filter_col_bylist(df_name, values, is_exclude, schema)
                required_cols.extend(cols_to_add)

        if filterby_range:
            cols_to_add = self.filter_col_byindex(df_name, index_range, is_exclude, schema)
            required_cols.extend(cols_to_add)
        
        required_cols = list(set(required_cols))
        code = "{0} = {1}.select({2})".format(df_name, df_name, required_cols)
        self.code_file.write(code + '\n')
        update_schema_after_filtercol(self,schema, required_cols, df_name)
        add_child_in_output(self,node_id,df_name)    

    def filter_col_bylist(self,df_name, values, is_exclude, schema):
        cols = list(schema.keys())
        if is_exclude:
            cols_to_add = []
            for i in cols:
                if i not in values:
                    cols_to_add.append(i)
        else:
            cols_to_add = values
        return cols_to_add
    

    def filter_col_bytype(self,df_name,values,is_exclude,schema):
        datatype_match = compare_datatype(schema)
        cols_to_add = []
        if not is_exclude:
            for i in values:
                cols_to_add.extend(datatype_match[i])
        else:
            cols_to_remove =[]
            for i in values:
                cols_to_remove.extend(datatype_match[i])
            for i in schema:
                if i not in cols_to_remove:
                    cols_to_add.append(i)
        return cols_to_add

    def filter_col_byindex(self,df_name,index_range,is_exclude,schema):
        cols = list(schema.keys())
        cols_to_add = []
        if not is_exclude:
            for ele in index_range:
                lower = ele[0]
                upper = ele[1]+1
                cols_to_add.extend(cols[lower:upper])
            cols_to_add = list(set(cols_to_add))
        else:
            cols_to_remove = []
            for ele in index_range:
                lower = ele[0]
                upper = ele[1]+1
                cols_to_remove.extend(cols[lower:upper])
            cols_to_remove = list(set(cols_to_remove))
            for i in cols:
                if i not in cols_to_remove:
                    cols_to_add.append(i)          
        return cols_to_add


    def join(self,node):
        node_id = node['id']
        join_type = str(node["parameters"].get("join type",'inner')).lower()
        left_prefix = node["parameters"].get("left prefix",'')
        right_prefix = node["parameters"].get("right prefix",'')
        join_columns =  node["parameters"].get("join columns")
        parents_id = self.pn_obj.child_parent[node_id]
        left_parent_name , right_parent_name  =self.find_left_right_parent(node_id,parents_id)
        left_parent_schema = self.cached_df_schema[left_parent_name]
        right_parent_schema = self.cached_df_schema[right_parent_name]
        df_name = left_parent_name+'_'+right_parent_name+'_'+'join'
        self.dataframe_name[node_id] = df_name 
        set_df_name_for_child(self,node_id,df_name)
        left_cols = list(left_parent_schema.keys())
        right_cols = list(right_parent_schema.keys()) 
        left_parent_name_alias =left_parent_name
        right_parent_name_alias = right_parent_name
        if left_prefix :
            left_cols = [left_prefix+col for col in left_cols]
            left_parent_name_alias = left_parent_name+'_alias'
            code = f"{left_parent_name_alias} = {left_parent_name}.toDF(*{left_cols})"
            self.code_file.write(code + '\n') 
        if right_prefix:
            right_cols = [right_prefix+col for col in right_cols]
            right_parent_name_alias = right_parent_name+'_alias'
            code = f"{right_parent_name_alias} = {right_parent_name}.toDF(*{right_cols})"
            self.code_file.write(code + '\n')
        condition,cols_to_remove = self.get_join_condition(left_cols,right_cols,join_columns,left_parent_name_alias,right_parent_name_alias,left_prefix,right_prefix)    
        code = f"{df_name} = {left_parent_name}.join({right_parent_name},[{condition}],'{join_type}')"
        self.code_file.write(code + '\n')   
        update_schema_after_join(self,left_parent_schema,right_parent_schema,left_cols,right_cols,cols_to_remove,df_name)
    

    def get_join_condition(self,leftcols,rightcols,join_columns,left_parent,right_parent,left_prefix,right_prefix):
        condition = ''
        cols_to_remove = []
        col = 0
        for con in join_columns:
            if con["left column"]['type'] == 'column':
                left_col_for_join =  con["left column"]['value'] 
                left_col_for_join = left_prefix+left_col_for_join
    
            if con["right column"]['type'] == 'column':
                right_col_for_join =  con["right column"]['value'] 
                right_col_for_join = right_prefix+right_col_for_join

            if con["left column"]['type'] == 'index':
                left_col_for_join = leftcols[con["left column"]['value']]

            if con["right column"]['type'] == 'index':
                right_col_for_join = rightcols[con["right column"]['value']] 
            
            left_col_for_join = left_parent + '.' +left_col_for_join 
            right_col_for_join = right_parent + '.' + right_col_for_join
            cols_to_remove.append(right_col_for_join)
            if col == len(join_columns)-1:
                condition  = condition + f"{left_col_for_join}=={right_col_for_join}"
            else:
                condition  = condition + f"{left_col_for_join}=={right_col_for_join}"  + ','    
            col = col+1    

        return condition,cols_to_remove



        
    def find_left_right_parent(self,node_id,parents_id):
        for ele in self.pn_obj.connections :
            from_id = ele["from"]["nodeId"]
            to_id = ele["to"]["nodeId"]
            if from_id in parents_id:
                if ele["from"]["portIndex"] == 0 and to_id == node_id:
                    left_parent_id = from_id
                    parents_id.remove(left_parent_id)
                    right_parent_id = parents_id[0]
                    break
                if ele["from"]["portIndex"] == 1 and to_id == node_id: 
                    right_parent_id = from_id
                    parents_id.remove(right_parent_id)
                    left_parent_id = parents_id[0]
                    break 
        return self.dataframe_name[left_parent_id],self.dataframe_name[right_parent_id]


    def call_method(self, operation_to_do, node_detail):
        method = operation_to_do
        return getattr(self, method)(node_detail)

    def get_case(self, case=[]):
        operation_to_do = case[0]
        node_detail = case[1]
        self.call_method(operation_to_do, node_detail)
