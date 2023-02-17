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
                child_df_name = name + '_b' + str(i)
                self.dataframe_name[children[i]] = child_df_name
        else:
            self.dataframe_name[children[0]] = name

#Reflect the division of branch in output file
def add_child_in_output(self,node_id,name):
    if node_id in self.pn_obj.relation_dict:
        children = self.pn_obj.relation_dict[node_id]
        if len(children) > 1:
            for i in range(len(children)):
                child_df_name = name + '_b' + str(i)
                self.code_file.write(f'{child_df_name}= {name}'+ '\n')


#Update the schema detail if the present schema is affected 
def update_schema_after_filtercol(self,schema,cols_to_add,df_name):
    new_schema = {}
    for i in schema:
        if i in cols_to_add:
            new_schema[i] = schema[i]
    self.cached_df_schema[df_name] = new_schema


def update_schema_after_join(self,schema1,schema2,left_cols,right_cols,cols_to_remove,df_name):
    new_schema = {}
    x = 0
    y = 0
    for i in schema1:
         col = left_cols[x]
         new_schema[col] =  schema1[i]
         x = x +1

    for j in schema2:
         if j not in cols_to_remove:
            col = right_cols[y]
            new_schema[col] =  schema2[j]
         y = y+1 
    self.cached_df_schema[df_name] = new_schema     


