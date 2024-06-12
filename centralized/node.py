import sys
import os
import time
import colorama
import threading
from concurrent import futures

# Inicialitzar colors terminal
colorama.init()

# Obtenir path del directori actual
current_dir = os.path.dirname(__file__)

# Importar classes gRPC
proto_path = os.path.join(current_dir, "..", "proto")
sys.path.append(proto_path)
import store_pb2, store_pb2_grpc

# Classe Node (pare de Master i Slaves)
class Node(store_pb2_grpc.KeyValueStoreServicer):
    
    # Constructor
    def __init__(self, id):
        self.id = id
        self.data = {}
        self.delay = 0
        # Lock per les lectures i escriptures
        self.lock = threading.Lock()
        
    # Mètode per guardar una dada
    def put(self, request, context):
        # Aquest mètode només l'implementarà el node Master
        pass
    
    # Mètode per obtenir una dada
    def get(self, request, context):
        key = request.key
        found = key in self.data
        # Fer lectura -------------
        self.lock.acquire()
        value = self.data.get(key, "")
        self.lock.release()
        # -------------------------
        self.log(f"get key={key} -> value={value}")
        time.sleep(self.delay)
        return store_pb2.GetResponse(value=value, found=found)
    
    # Mètode per afegir delay a un node en concret
    def slowDown(self, request, context):
        self.delay = request.seconds
        self.log(f"delayed with {self.delay} sec")
        return store_pb2.SlowDownResponse(success=True)
    
    # Mètode per resetejar el delay d'un node en concret
    def restore(self, request, context):
        self.delay = 0
        self.log(f"delayed restored")
        return store_pb2.SlowDownResponse(success=True)
    
    # Mètode per iniciar un 2PC
    def canCommit(self, request, context):
        # Aquest mètode només l'implementaran els Slaves
        pass
    
    # Mètode per confirmar un 2PC
    def doCommit(self, request, context):
        # Aquest mètode només l'implementaran els Slaves
        pass
    
    # Mètode per fer una escriptura mitjançant Locks
    def write_value(self, key, value):
        self.lock.acquire()
        self.data[key] = value
        self.lock.release()
    
    # Mètode per mostrar els logs
    def log(self, msg):
        print(f"{colorama.Fore.YELLOW}[{self.id}]{colorama.Fore.RESET} {msg}")
        
