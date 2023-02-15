import json
class Get_Root_Node:

    def __init__(self, data):
        self.data = data
        self.opId_of_read_data = "1a3b32f0-f56d-4c44-a396-29d2dfd43423"
        self.operations = data['workflow']['nodes']
        self.connections = data['workflow']['connections']
        self.read_operations = self.get_all_read_operations()
        self.relation_dict = self.generate_parent_child()
        self.child_parent = self.generate_child_parent()
        self.parent_node = self.find_1st_node_id()

    def get_all_read_operations(self):
        read_operations_only = []
        for node in self.operations:
            if node['operation']['id'] == self.opId_of_read_data:
                read_operations_only.append(node['id'])
        return read_operations_only

    def generate_parent_child(self):
        relation_dict = {}
        for con in self.connections:
            from_nodeid = con['from']['nodeId']
            to_nodeid = con['to']['nodeId']
            if from_nodeid not in relation_dict:
                children = self.get_children(from_nodeid)
                relation_dict[from_nodeid] = children
        return relation_dict

    def generate_child_parent(self):
        relation_dict = {}
        for con in self.connections:
            from_nodeid = con['from']['nodeId']
            to_nodeid = con['to']['nodeId']
            if to_nodeid not in relation_dict:
                parent = self.get_parent(to_nodeid)
                relation_dict[to_nodeid] = parent
        return relation_dict

    def get_children(self, parent):
        children = []
        for con in self.connections:
            from_nodeid = con['from']['nodeId']
            to_nodeid = con['to']['nodeId']
            if parent == from_nodeid:
                children.append(to_nodeid)
        return children

    def get_parent(self, child):
        parent = []
        for con in self.connections:
            from_nodeid = con['from']['nodeId']
            to_nodeid = con['to']['nodeId']
            if child == to_nodeid:
                parent.append(from_nodeid)
        return parent

    def find_1st_node_id(self):
        if len(self.read_operations) == 1:
            return self.read_operations
        else:
            valid_1st_node = []
            for i in self.read_operations:
                valid = True
                for child in self.relation_dict[i]:
                    for ele in self.relation_dict:
                        if child in self.relation_dict[ele] and ele not in self.read_operations:
                            valid = False
                if valid:
                    valid_1st_node.append(i)
        return valid_1st_node


