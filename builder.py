# Generate dict with datasource_id as key & datasource as value
def generate_datasource_dict(self):
    if self.workflow_data["thirdPartyData"]['datasources']:
        self.is_datasource_inhand = True
        for data in self.workflow_data["thirdPartyData"]['datasources']:
            datasource_id = data["id"]
            self.datasource_dict[datasource_id] = data

# Generate dict with node_id as key & df(default name) as value
def generate_dataframe_name(self):
    for i in self.pn_obj.operations:
        node_id = i['id']
        self.operation_ids.append(node_id)
        self.dataframe_name[node_id] = 'df'

#Set the df_name for children of given node
def set_df_name_for_child(self, node, name):
    if node in self.pn_obj.relation_dict:
        children = self.pn_obj.relation_dict[node]
        if len(children) > 1:
            for i in range(len(children)):
                child_df_name = name + '_b' + str(i)
                self.dataframe_name[children[i]] = child_df_name
                self.code_file.write(f'{child_df_name}= {name}'+ '\n')
        else:
            self.dataframe_name[children[0]] = name

#Update the schema detail
def update_schema(self,schema,cols_to_add,df_name):
    new_schema = {}
    for i in cols_to_add:
        new_schema[i] = schema[i]
    self.cached_df_schema[df_name] = new_schema