# Generate dict with datasource_id as key & datasource as value
def generate_datasource_dict(self):
    if self.workflow_data["thirdPartyData"]['datasources']:
        self.is_datasource_inhand = True
        for data in self.workflow_data["thirdPartyData"]['datasources']:
            datasource_id = data["id"]
            self.datasource_dict[datasource_id] = data
    else:
        print("------- Could not find datasource.Give the details in order -----")
# Generate dict with node_id as key & df(default name) as value
def generate_dataframe_name(self):
    for i in self.pn_obj.operations:
        node_id = i['id']
        self.operation_ids.append(node_id)
        self.dataframe_name[node_id] = 'df'

#Set the df_name for children of given node
def set_df_name_for_child(self, node_id, name):
    if node_id in self.pn_obj.relation_dict:
        children = self.pn_obj.relation_dict[node_id]
        if len(children) > 1:
            for i in range(len(children)):
                    child_df_name = name + '_b' + str(i+1)
                    self.dataframe_name[children[i]] = child_df_name
        else:
            self.dataframe_name[children[0]] = name

#Reflect the division of branch in output file
def add_child_in_output(self,node_id,name):
    if node_id in self.pn_obj.relation_dict:
        children = self.pn_obj.relation_dict[node_id]
        if len(children) > 1:
            for i in range(len(children)):
                op_id = self.node_id_op_id[children[i]]
                if op_id != "06374446-3138-4cf7-9682-f884990f3a60":
                    child_df_name = name + '_b' + str(i+1)
                    self.code_file.write(f'{child_df_name}= {name}'+ '\n')



#Update the schema detail if the present schema is affected 
def update_schema_after_filtercol(self,schema,cols_to_add,df_name):
    new_schema = {}
    for i in schema:
        if i in cols_to_add:
            new_schema[i] = schema[i]
    self.cached_df_schema[df_name] = new_schema

#Update the schema after Join
def update_schema_after_join(self,schema1,schema2,left_cols,right_cols,cols_to_remove,df_name,right_prefix):
    new_schema = {}
    x = 0
    y = 0
    for i in schema1:
         col = left_cols[x]
         new_schema[col] =  schema1[i]
         x = x +1

    for j in schema2:
         updated_col = right_prefix + j
         if  updated_col not in cols_to_remove:
            col = right_cols[y]
            new_schema[col] =  schema2[j]
         y = y+1    
    self.cached_df_schema[df_name] = new_schema    


#Called if we have option to select columns using names,index,datatype
def fetch_columns_to_add(self,selections,is_exclude,schema):
    index_range = []
    filterby_range = False
    req_col = []
    for ele in selections:
        selection_type = ele['type']
        values = ele['values']
        if selection_type == 'indexRange':
            filterby_range = True
            index_range.append(values)
        if selection_type == 'typeList':
            cols_to_add = self.filter_col_bytype(values,is_exclude,schema)
            req_col.extend(cols_to_add)
        if selection_type == 'columnList':
            cols_to_add = self.filter_col_bylist(values, is_exclude, schema)
            req_col.extend(cols_to_add)

        if filterby_range:
            cols_to_add = self.filter_col_byindex(index_range, is_exclude, schema)
            req_col.extend(cols_to_add)

    required_cols = []
    [required_cols.append(x) for x in req_col if x not in required_cols]  
    return required_cols
      
#Find respective left ,right parent
def find_left_right_parent(self,node_id,parents_id):
    for ele in self.pn_obj.connections :
        from_id = ele["from"]["nodeId"]
        to_id = ele["to"]["nodeId"]
        if from_id in parents_id:
            if ele["to"]["portIndex"] == 0 and to_id == node_id:
                left_parent_id = from_id
                parents_id.remove(left_parent_id)
                right_parent_id = parents_id[0]
                break
            if ele["to"]["portIndex"] == 1 and to_id == node_id: 
                right_parent_id = from_id
                parents_id.remove(right_parent_id)
                left_parent_id = parents_id[0]
                break 
    return self.dataframe_name[left_parent_id],self.dataframe_name[right_parent_id]       
     


def fetch_user_defined_missing_value(user_defined_missing_values):
    values_to_consider = []
    for ele in  user_defined_missing_values :
        if len(ele) >0 :
            values_to_consider.append(ele["missing value"])
    return  values_to_consider  

def update_schema_after_sql_transformation(schema,formula):
    new_schema = {}
    existing_cols = list(schema.keys())
    formula = formula.strip()
    from_index = formula.lower().find('from') 
    select_index = formula.lower().find('Select') + 7

    list_cols = formula[select_index:from_index].split(',')
    new_list = []
    for i in range(len(list_cols)):
        if '(' in list_cols[i] and ')' not in list_cols[i] :
            y = list_cols[i] + ','+list_cols[i+1]
            new_list.append(y)
        elif '(' not in list_cols[i] and ')' in list_cols[i]:
            pass    
        else:
            new_list.append(list_cols[i])

       
    for i in new_list:
        i = i.strip()
        if '*' in i:
            for i in schema:
                new_schema[i] = schema[i]
        for j in existing_cols:
            if j.lower() in i.lower():
                if 'as' in i.lower():
                    as_index = i.lower().find('as')
                    alias = i[as_index+2:].strip()
                    new_schema[alias] = schema[j]
                else:
                    if j.lower() == i.lower():
                        new_schema[j] = schema[j]   
                    else:
                        i = i[:from_index].strip()
                        new_schema[i] = schema[j]     

    print(new_schema,'lkjhg')                                     
    return new_schema               

