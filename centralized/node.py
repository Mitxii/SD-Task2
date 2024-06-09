import sys
import os
import time
import colorama
from concurrent import futures

# Inicialitzar colors terminal
colorama.init()

current_dir = os.path.dirname(__file__)

# Importar classes gRPC
proto_path = os.path.join(current_dir, "..", "proto")
sys.path.append(proto_path)
import store_pb2, store_pb2_grpc

# Classe Node
class Node(store_pb2_grpc.KeyValueStoreServicer):
    
    def __init__(self, id):
        self.id = id
        self.data = {}
        self.delay = 0
        self.is_available = True
        
    def put(self, request, context):
        # Aquest mètode només l'implementarà el node Master
        pass
        
    def get(self, request, context):
        key = request.key
        value = self.data.get(key, "")
        found = key in self.data
        self.log(f"get key={key} -> value={value}")
        time.sleep(self.delay)
        return store_pb2.GetResponse(value=value, found=found)
    
    def slowDown(self, request, context):
        self.delay = request.seconds
        self.log(f"delayed with {self.delay} sec")
        time.sleep(self.delay)
        return store_pb2.SlowDownResponse(success=True)
    
    def restore(self, request, context):
        self.delay = 0
        self.log(f"delayed restored")
        return store_pb2.SlowDownResponse(success=True)
    
    def canCommit(self, request, context):
        # Aquest mètode serà sobreescrit pels Slaves
        return store_pb2.CommitResponse(can_commit=True)
    
    def doCommit(self, request, context):
        key = request.key
        value = request.value
        self.data[key] = value
        self.log(f"set key={key}, value={value}")
        time.sleep(self.delay)
        self.is_available = True
        return store_pb2.Empty()
    
    def doAbort(self, request, context):
        time.sleep(self.delay)
        self.is_available = True
        return store_pb2.Empty()
    
    def log(self, msg):
        print(f"{colorama.Fore.YELLOW}[{self.id}]{colorama.Fore.RESET} {msg}")
        
