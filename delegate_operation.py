import root_node
import re
import random
from helper import *
from constants import *
from builder import *


class Node_Operation:

    def __init__(self, data, code_file, pn_obj):
        self.workflow_data = data
        self.code_file = code_file
        self.pn_obj = pn_obj
        self.node_id_op_id = self.pn_obj.node_id_op_id
        self.is_datasource_inhand = False
        self.datasource_dict = {}
        self.dataframe_name = {}
        self.cached_df_schema = {}
        self.operation_ids =[]
        generate_dataframe_name(self)
        generate_datasource_dict(self)
        self.read_count = 1
        self.datasource_ids = {}
        self.datasource_read_count = {}
        self.s3_path = ''


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

    def read_dataframe(self, node):
        datasource_id = node['parameters']['data source']
        node_id = node['id']
        if datasource_id  not in self.datasource_ids :
            if self.is_datasource_inhand:
                datasource = self.datasource_dict[datasource_id]
                data_from = datasource['params']['datasourceType'] + "Params"
                file_format = datasource['params'][data_from]['fileFormat']
                datasource_name = datasource['params']['name']
                datasource_name = return_valid_df_name(datasource_name)
                if datasource_name not in self.dataframe_name:
                    url = input("Give file location of {0} data ".format(datasource_name)).strip()
                    url = cleanse_data(url)
                    split_url = url.split('/')
                    if 's3' in split_url[0]:
                        self.s3_path  = split_url[0]+'//'+split_url[2]
                    self.get_schema_details_from_user(datasource_name)
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
                datasource_name = cleanse_data(datasource_name)
                datasource_name = return_valid_df_name(datasource_name)
                url = input("Give file location of data_{0} ".format(self.read_count)).strip()
                url = cleanse_data(url)
                self.get_schema_details_from_user(datasource_name)
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

            self.code_file.write(code + '\n')
            self.datasource_ids[datasource_id] = datasource_name
            self.datasource_read_count[datasource_name] = 1
            set_df_name_for_child(self,node_id, datasource_name)


        else:
           datasource_name = self.datasource_ids[datasource_id] 
           schema = self.cached_df_schema[datasource_name] 
           self.datasource_read_count[datasource_name]
           self.datasource_read_count[datasource_name] = self.datasource_read_count[datasource_name] +1
           datasource_name = datasource_name+ '_r' + str(self.datasource_read_count[datasource_name])
           code = f'{datasource_name } = {self.datasource_ids[datasource_id]}'
           self.code_file.write(code + '\n')

           self.cached_df_schema[datasource_name] = schema

        self.dataframe_name[node['id']] = datasource_name
        set_df_name_for_child(self,node_id, datasource_name)

        

    def write_dataframe(self, node):
        node_id = node['id']
        datasource_id = node['parameters']['data source']
        df_name = self.dataframe_name[node_id]
        parents_id = self.pn_obj.child_parent[node_id]
        parent_name = self.dataframe_name[parents_id[0]]
        if self.is_datasource_inhand:
            datasource = self.datasource_dict[datasource_id]
            data_from = datasource['params']['datasourceType'] + "Params"
            file_format = datasource['params'][data_from]['fileFormat']
            datasource_name = datasource['params']['name']
            url = datasource['params'][data_from]['libraryPath']
            
            if 'library' in url:
                url = url.split('://')[-1] + '_'+str(random.randint(1,30))
                if self.s3_path:
                    url = self.s3_path + '/' + url
                print(f"-----  Output for  {datasource_name} will be in: "+url+'   --------')
        else:
            url = input(f"Give file location to save {df_name}").strip()
            url = cleanse_data(url)
            file_format = input(f"Give file_format of {df_name} ").strip()
            file_format = cleanse_data(file_format)
        if file_format == 'csv':
            code = f"{parent_name}.write.mode('overwrite').option('header',True).option('delimiter',',').option('inferschema',True).csv('{url}')"
        else:
            code = f"{parent_name}.write.mode('overwrite').{file_format}('{url}')"
        self.code_file.write(code + '\n')

    def filter_rows(self, node):
        node_id = node['id']
        df_name = self.dataframe_name[node_id]
        set_df_name_for_child(self,node_id,df_name)
        condition = node["parameters"]['condition']
        parents_id = self.pn_obj.child_parent[node_id]
        parent_name = self.dataframe_name[parents_id[0]]
        if condition != '':
            condition = return_valid_exp(condition)
            condition = condition.replace("'",'"')
            code = f"{df_name} = {parent_name}.filter('{condition}')"
        else:
            code =  f"{df_name} = {parent_name}"
        self.code_file.write(code + '\n')
        #get the parent of this and update that as schema as no change in cols
        schema = self.cached_df_schema[parent_name]
        self.cached_df_schema[df_name] = schema


    def filter_columns(self, node):
        node_id = node['id']
        df_name = self.dataframe_name[node_id]
        set_df_name_for_child(self,node_id,df_name)
        parents_id = self.pn_obj.child_parent[node_id]
        parent_name = self.dataframe_name[parents_id[0]]
        schema = self.cached_df_schema[parent_name]
        selections = node['parameters']["selected columns"]['selections']
        is_exclude = node['parameters']["selected columns"]['excluding']
        cols = fetch_columns_to_add(self,selections,is_exclude,schema)
        required_cols = []
        for i in schema:
            if i in cols:
                required_cols.append(i)

        code = f"{df_name} = {parent_name}.select({required_cols})"
        self.code_file.write(code + '\n')
        update_schema_after_filtercol(self,schema, required_cols, df_name)

    def filter_col_bylist(self, values, is_exclude, schema):
        cols = list(schema.keys())
        cols_to_add = []
        if is_exclude:
            for i in cols:
                if i not in values:
                    cols_to_add.append(i)
        else:
           cols_to_add = values
        return cols_to_add
    

    def filter_col_bytype(self,values,is_exclude,schema):
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

    def filter_col_byindex(self,index_range,is_exclude,schema):
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

    def union(self,node):
        node_id = node['id']
        df_name = self.dataframe_name[node_id]
        parents_id = self.pn_obj.child_parent[node_id] 
        left_parent_name , right_parent_name  = find_left_right_parent(self,node_id,parents_id)
        df_name = left_parent_name +'_'+ right_parent_name +'_union'
        if df_name in self.dataframe_name.values():
            df_name = df_name + str(random.randint(1,30))
        code = f"{df_name} = {left_parent_name }.union({right_parent_name })"
        self.code_file.write(code + '\n')
        set_df_name_for_child(self,node_id,df_name)
        schema = self.cached_df_schema[left_parent_name ]
        self.dataframe_name[node_id] = df_name
        self.cached_df_schema[df_name] = schema


    def join(self,node):
        node_id = node['id']
        join_type = node["parameters"].get("join type")
        if join_type:
            join_type = list(join_type.keys())[0]
        else:
            join_type  = 'Join'
        left_prefix = node["parameters"].get("left prefix",'')
        right_prefix = node["parameters"].get("right prefix",'')
        join_columns =  node["parameters"].get("join columns")
        parents_id = self.pn_obj.child_parent[node_id] 
        left_parent_name , right_parent_name  = find_left_right_parent(self,node_id,parents_id)
        left_parent_schema = self.cached_df_schema[left_parent_name]
        right_parent_schema = self.cached_df_schema[right_parent_name]
        df_name = left_parent_name+'_'+right_parent_name+'_'+'join'
        if df_name in self.dataframe_name.values():
            df_name = df_name + str(random.randint(1,30))
        self.dataframe_name[node_id] = df_name 
        set_df_name_for_child(self,node_id,df_name)
        left_cols = list(left_parent_schema.keys())
        right_cols = list(right_parent_schema.keys())
        if left_parent_name == right_parent_name:
            right_parent_name = right_parent_name + '_right'
            code = f'{right_parent_name}={left_parent_name}'
            self.code_file.write(code + '\n')
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
        join_type = match_join_type(join_type)    
        condition,cols_to_remove,drop_cols = self.get_join_condition(left_cols,right_cols,join_columns,left_parent_name_alias,right_parent_name_alias,left_prefix,right_prefix)    
        code = f"{df_name} = {left_parent_name_alias}.alias('left').join({right_parent_name_alias}.alias('right'),[{condition}],'{join_type}'){drop_cols}"
        self.code_file.write(code + '\n')   
        update_schema_after_join(self,left_parent_schema,right_parent_schema,left_cols,right_cols,cols_to_remove,df_name,right_prefix)
        
    def get_join_condition(self,leftcols,rightcols,join_columns,left_parent,right_parent,left_prefix,right_prefix):
        condition = ''
        cols_to_remove = []
        col = 0
        drop_cols =''
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
            
            cols_to_remove.append(right_col_for_join)
            if right_col_for_join not in drop_cols:
                drop_cols = drop_cols +f".drop({right_parent}.{right_col_for_join})"
            left_col_for_join = f"col('left.{left_col_for_join}')"
            right_col_for_join = f"col('right.{right_col_for_join}')"
            if col == len(join_columns)-1:
                condition  = condition + f"{left_col_for_join}=={right_col_for_join}"
            else:
                condition  = condition + f"{left_col_for_join}=={right_col_for_join}"  + ','    
            col = col+1    

        return condition,cols_to_remove,drop_cols

    
    def sort(self,node):
        node_id = node['id']
        sort_columns = node['parameters']['sort columns']
        df_name = self.dataframe_name[node_id]
        set_df_name_for_child(self,node_id,df_name)
        parents_id = self.pn_obj.child_parent[node_id]
        parent_name = self.dataframe_name[parents_id[0]]
        schema = self.cached_df_schema[parent_name]
        parent_columns = list(schema.keys())
        sort_col = []
        order_type = []
        for ele in sort_columns:
            is_descending = ele.get("descending",False)
            if is_descending:
                order_type.append(0)
            else:
                order_type.append(1)   
            if ele["column name"]["type"] == "column":
                sort_col.append(ele["column name"]["value"])
            else:
                sort_col.append(parent_columns[ele["column name"]["value"]])

        code = f"{df_name} = {parent_name}.sort({sort_col},ascending={order_type})"
        self.code_file.write(code + '\n')
        #get the parent of this and update that as schema as no change in cols
        self.cached_df_schema[df_name] = schema



    def sql_column_transformation(self,node):
        node_id = node['id']
        df_name = self.dataframe_name[node_id]
        set_df_name_for_child(self,node_id,df_name)
        formula = node['parameters'].get('formula','')
        formula  = return_valid_exp(formula)
        parents_id = self.pn_obj.child_parent[node_id]
        parent_name = self.dataframe_name[parents_id[0]]
        schema = self.cached_df_schema[parent_name]
        parent_columns = list(schema.keys())
        input_col_alias = node['parameters'].get("input column alias",'x')
        transformation = ''
        new_schema ={}
        for i in schema:
            new_schema[i] = schema[i]

        operate_on = node['parameters']['operate on']
        if 'one column' in operate_on:  
            new_schema,transformation = self.sql_column_operate_one_column(operate_on,parent_columns,schema,input_col_alias,formula,new_schema)
        if 'multiple columns' in operate_on:
            new_schema,transformation = self.sql_column_operate_multi_columns(operate_on,schema,formula,input_col_alias,new_schema)
        code = f"{df_name} = {parent_name}{transformation}"
        self.code_file.write(code + '\n')
        self.cached_df_schema[df_name] = new_schema



    def sql_column_operate_one_column(self,operate_on,parent_columns,schema,input_col_alias,formula,new_schema):
        new_col_alias =''
        transformation = ''
        if operate_on['one column'].get('output'):
            if operate_on['one column']['output'].get('append new column'):
                new_col_alias = operate_on['one column']['output']['append new column']['output column']
        if operate_on['one column']['input column']['type'] == 'column':
            col = operate_on['one column']['input column']['value']
        else:
            col = parent_columns[operate_on['one column']['input column']['value']]               
        column = new_col_alias+col
        if col in schema:
            new_schema[column] = schema[col]

        if formula:
            col = check_column_is_valid(col)
            x = formula.find('(',1)
            formula = formula[:x] + formula[x:].replace(input_col_alias,col)
            transformation = f'.withColumn("{column}",expr("{formula}"))'
        else:
            transformation = f'.withColumn("{column}",col("{col}"))'   
        return new_schema,transformation
            
    def sql_column_operate_multi_columns(self,operate_on,schema,formula,input_col_alias,new_schema):
        new_col_alias = ''
        transformation = ''
        multi_col = operate_on['multiple columns'] 
        if multi_col.get('output'):
            if multi_col['output'].get('append new columns'):
                new_col_alias = operate_on['multiple columns']['output']['append new columns']['column name prefix']
        selections = multi_col['input columns']['selections']
        is_exclude = multi_col['input columns']['excluding']
        required_cols = fetch_columns_to_add(self,selections,is_exclude,schema)
        if formula:
            x = formula.find('(',1)
        for col in required_cols:
            column = new_col_alias+col
            if col in schema:
                new_schema[column] = schema[col]
            if formula:
                col = check_column_is_valid(col)
                formula1 = formula[:x] + formula[x:].replace(input_col_alias,col) 
                transformation = transformation + f'.withColumn("{column}",expr("{formula1}"))'
            else:
                transformation = transformation + f'.withColumn("{column}",col("{col}"))' 
        return new_schema,transformation
    
    def sql_transformation(self,node):
        new_schema = {}
        node_id = node['id']
        df_name = self.dataframe_name[node_id]
        set_df_name_for_child(self,node_id,df_name)
        expression = node[ "parameters"].get('expression','')
        parents_id = self.pn_obj.child_parent[node_id]
        parent_name = self.dataframe_name[parents_id[0]]
        schema = self.cached_df_schema[parent_name]
        df = node[ "parameters"].get('dataframe id','df')
        if expression:
            expression = return_valid_exp(expression)
            register_table = f'{parent_name}.registerTempTable("{df}")'
            self.code_file.write(register_table + '\n')
            code = f'{df_name} = spark.sql("{expression}")'
            self.code_file.write(code + '\n')

        new_schema = update_schema_after_sql_transformation(schema,expression)  
        self.cached_df_schema[df_name] = new_schema

    def sql_combine(self,node):
        new_schema = {}
        node_id = node['id']
        left_df_id = node['parameters']["Left dataframe id"]
        right_df_id = node['parameters']["Right dataframe id"]
        expression = node[ "parameters"].get('expression','')
        parents_id = self.pn_obj.child_parent[node_id] 
        left_parent_name , right_parent_name  = find_left_right_parent(self,node_id,parents_id)
        left_parent_schema = self.cached_df_schema[left_parent_name]
        right_parent_schema = self.cached_df_schema[right_parent_name]
        df_name = left_parent_name+'_'+right_parent_name+'_'+'sql_combine'
        if df_name in self.dataframe_name.values():
            df_name = df_name + str(random.randint(1,30))
        self.dataframe_name[node_id] = df_name 
        set_df_name_for_child(self,node_id,df_name)
        if expression:
            register_left_table = f'{left_parent_name}.registerTempTable("{left_df_id}")'
            self.code_file.write(register_left_table + '\n')
            register_right_table = f'{right_parent_name}.registerTempTable("{right_df_id}")'
            self.code_file.write(register_right_table + '\n')
            code = f'{df_name} = spark.sql("{expression}")'
            self.code_file.write(code + '\n')
        new_schema = update_schema_after_sql_combine(left_parent_schema ,right_parent_schema ,expression,left_df_id, right_df_id)  
        self.cached_df_schema[df_name] = new_schema    
        
    def projection(self,node):
        project_schema = {}
        node_id = node['id']
        df_name = self.dataframe_name[node_id]
        set_df_name_for_child(self,node_id,df_name)
        projection_columns = node["parameters"]["projection columns"]
        parents_id = self.pn_obj.child_parent[node_id]
        parent_name = self.dataframe_name[parents_id[0]]
        schema = self.cached_df_schema[parent_name]
        parent_columns = list(schema.keys())
        rename_condition = ''
        for ele in projection_columns:
            col = ''
            if ele["original column"]["type"] == 'column':
                col = ele["original column"]["value"]
            else:
                col =  parent_columns[ele["original column"]["value"]] 
            col_datatype = schema[col]
            is_rename_col = ele.get("rename column")
            if is_rename_col:
                rename_to = is_rename_col["Yes"]["column name"]
                rename_condition =  rename_condition  + f"col('{col}').alias('{rename_to}')" + ','
                project_schema[rename_to] = col_datatype

            else:
                rename_condition = rename_condition  +f"'{col}'" + ','
                project_schema[col] = col_datatype

        code = f'{df_name} = {parent_name}.alias("df").select({rename_condition})'
        self.code_file.write(code + '\n')
        self.cached_df_schema[df_name] = project_schema
        

    def handle_missing_value(self,node):
        node_id = node['id']
        df_name = self.dataframe_name[node_id]
        set_df_name_for_child(self,node_id,df_name)
        columns = node["parameters"]["columns"]
        strategy = node["parameters"].get("strategy","remove row")
        missing_value_indicator = node["parameters"].get("missing value indicator")
        user_defined_missing_values = node["parameters"]["user-defined missing values"]
        parents_id = self.pn_obj.child_parent[node_id]
        parent_name = self.dataframe_name[parents_id[0]]
        schema = self.cached_df_schema[parent_name]
        is_exclude = columns['excluding']
        selections = columns['selections']
        required_cols = fetch_columns_to_add(self,selections,is_exclude,schema)
        values_to_consider = fetch_user_defined_missing_value(user_defined_missing_values)
        self.findType(required_cols,schema)
        code = f"{df_name} = {parent_name}"
        code = self.add_missing_value_indicator(code,missing_value_indicator,values_to_consider,required_cols)
        code = self.apply_stratergy(code,strategy,values_to_consider,required_cols)
        #if missing value indicator added reflected in schema
        new_schema = {}
        for key in schema.keys():
            new_schema[key]= schema[key]
        for key  in self.schema_update.keys():
            new_schema[key]= self.schema_update[key]
        self.code_file.write(code + '\n')
        self.cached_df_schema[df_name] = new_schema

        
    

    def findType(self,required_cols,schema):
        schema_of_req_col = {}
        for i in required_cols:
            schema_of_req_col[i] = schema[i]
        datatype_match = compare_datatype(schema_of_req_col)
        self.numeric_type = datatype_match['numeric']
        self.string_type  = datatype_match['string']
        self.bool_type =  datatype_match['boolean']
     
        
    def apply_stratergy(self,code,strategy,values_to_consider,required_cols): 

        if "replace with custom value" in strategy:
            code = self.replace_with_custom_value(code,strategy,values_to_consider,required_cols)
        if "remove row" in strategy: 
            code = self.remove_row(code,values_to_consider,required_cols)   
        
        return code     

    def replace_with_custom_value(self,code,strategy,values_to_consider,required_cols):
        new_value = strategy["replace with custom value"]["value"]
        if len(self.numeric_type) > 0 :
            code = code + f".fillna({new_value},{required_cols})"
            if len(self.string_type) > 0:
                new_value = "'"+new_value+"'"
                code = code + f".fillna({new_value},{required_cols})"
        #User_defined_missing_value
        if len(values_to_consider) > 0:
            # Column has datatype that will be numeric only as replace there 
            
            if len(self.numeric_type) > 0 :
                val = []
                for i in values_to_consider:
                    try:
                        val.append(int(i))
                    except:
                        pass
                values_to_consider = val        
            else:
                new_value = "'"+new_value+"'"
            code = code +f".na.replace({values_to_consider},{new_value},{required_cols})"    
        else:
            if len(self.string_type) > 0 :
                 new_value = "'"+new_value+"'"
            code = code + f".fillna({new_value},{required_cols})"
        return code
         
    def remove_row(self,code,values_to_consider,required_cols):
            #User_defined_missing_value
        code = code+ f".dropna('any',subset={required_cols})"
        if len(values_to_consider) > 0:
            for i in required_cols:    
                code = code+ f".filter(~col('{i}').isin({values_to_consider}))"
        return code

    def add_missing_value_indicator(self,code,missing_value_indicator,values_to_consider,required_cols):
        self.schema_update = {}
        if missing_value_indicator and "Yes"  in missing_value_indicator:
            prefix = missing_value_indicator['Yes']["indicator column prefix"]
            for i in required_cols:
                new_col_name = prefix+i
                self.schema_update[new_col_name] = 'boolean'
                condition = ''
                if len(values_to_consider) >0:
                    condition = f"when(col('{i}').isin({values_to_consider}),True)"
                    condition = condition + f'.when(col("{i}").isNull(),True).otherwise(False)'    
                    code = code + f".withColumn('{new_col_name}',{condition})" 
                else:
                    condition = condition + f'when(col("{i}").isNull(),True).otherwise(False)'    
                    code = code + f".withColumn('{new_col_name}',{condition})"

        return code    
        

    def call_respective_operation(self, operation_to_do, node_detail):
        method = operation_to_do
        return getattr(self, method)(node_detail)

    def get_operation(self, case=[]):
        operation_to_do = case[0]
        node_detail = case[1]
        self.call_respective_operation(operation_to_do, node_detail)
