import json
from constants import *
import delegate_operation
import root_node

workflow_path = 'Workflow_CaseStudy.json'
f = open(workflow_path)
code_file = open('code_output', 'w')

data = json.load(f)
pn_obj = root_node.Get_Root_Node(data)
del_obj = delegate_operation.Node_Operation(data, code_file, pn_obj)


class Generate_SparkCode:
    def __init__(self):
        self.generate_dict_operation()
        self.generate_isvisited_dict()
        self.execuete_read_operations()
        for i in pn_obj.parent_node:
            self.BFS(i)

    def generate_isvisited_dict(self):
        self.visited = {}
        for ele in pn_obj.operations:
            self.visited[ele['id']] = False

    def generate_dict_operation(self):
        self.dict_operation = {}
        for i in pn_obj.operations:
            node_id = i['id']
            self.dict_operation[node_id] = i

    def execuete_read_operations(self):
        for node_id in pn_obj.read_operations:
            node_details = self.dict_operation[node_id]
            operation_id = node_details['operation']['id']
            operation_to_do = operations_dict[operation_id]
            fun_to_call = [operation_to_do, node_details, data]
            delegate_operation.Node_Operation.get_case(del_obj, fun_to_call)
            self.visited[node_id] = True


    def is_all_parent_visited(self,child):
        all_parents = pn_obj.child_parent[child]
        can_execute_child = True
        for id in all_parents:
            if self.visited[id] == False:
                can_execute_child = False
                break
        return can_execute_child

    def BFS(self, node):
        queue = []
        queue.append(node)
        self.visited[node] = True
        while queue:
            
            s = queue.pop(0)
            print('order of execution', s)
            if s in pn_obj.relation_dict:
                for child in pn_obj.relation_dict[s]:
                    can_execute_child = self.is_all_parent_visited(child)
                    if can_execute_child:
                        if not self.visited[child]:
                            queue.append(child)
                            node_details = self.dict_operation[child]
                            operation_id = node_details['operation']['id']
                            operation_to_do = operations_dict[operation_id]
                            fun_to_call = [operation_to_do, node_details]
                            delegate_operation.Node_Operation.get_case(del_obj, fun_to_call)
                            self.visited[child] = True


job = Generate_SparkCode()
print("Schema:",del_obj.cached_df_schema)
print("Df name:",del_obj.dataframe_name )

