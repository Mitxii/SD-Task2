import sys
import os
import grpc
import time
from concurrent import futures

current_dir = os.path.dirname(__file__)

# Importar classes gRPC
proto_path = os.path.join(current_dir, "..", "proto")
sys.path.append(proto_path)
import store_pb2
import store_pb2_grpc

# Classe Node
class Node(store_pb2_grpc.KeyValueStoreServicer):
    
    def __init__(self):
        self.data = {}
        self.delay = 0
        
    def put(self, request, context):
        # Aquest mètode només l'implementarà el node Master
        pass
        
    def get(self, request, context):
        time.sleep(self.delay)
        key = request.key
        value = self.data.get(key, "")
        found = key in self.data
        print(f"GET [{key}, {value}]")
        return store_pb2.GetResponse(value=value, found=found)
    
    def slowDown(self, request, context):
        self.delay = request.seconds
        return store_pb2.SlowDownResponse(success=True)
    
    def restore(self, request, context):
        self.delay = 0
        return store_pb2.SlowDownResponse(success=True)
    
    def canCommit(self, request, context):
        # Aquest mètode serà sobreescrit pels Slaves
        return store_pb2.CommitResponse(can_commit=True)
    
    def doCommit(self, request, context):
        key = request.key
        value = request.value
        self.data[key] = value
        print(f"SET [{key}, {value}]")
        return store_pb2.Empty()
        
