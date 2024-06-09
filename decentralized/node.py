import grpc
import sys
import os
import time
import colorama
import signal
import json
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
    def __init__(self, id, weight):
        self.id = id
        self.weight = weight
        self.data = {}
        self.delay = 0
        # Altres nodes pel quorum
        self.other_nodes = []
        
    # Mètode per guardar una dada
    def put(self, request, context):
        key = request.key
        value = request.value
        
        # Protocol Quorum
        for other_stub in self.other_nodes:
            response = other_stub.askVote(store_pb2.AskVoteRequest(key=key))
        
        pass
    
    # Mètode per obtenir una dada
    def get(self, request, context):
        key = request.key
        value = self.data.get(key, "")
        found = key in self.data
        self.log(f"get key={key} -> value={value}")
        time.sleep(self.delay)
        return store_pb2.GetResponse(value=value, found=found)
    
    def quorum(self):
        pass
    
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
    
    # Mètode per enviar el pes durant un Quorum
    def askVote(self, request, context):
        return store_pb2.AskVoteResponse(weight=self.weight)
    
    # Mètode per registrar un Node i passar-li l'estat actual
    def registerNode(self, request, context):
        other_address = request.address
        channel = grpc.insecure_channel(other_address)
        slave_stub = store_pb2_grpc.KeyValueStoreStub(channel)
        self.other_nodes.append(slave_stub)
        self.log(f"Node registrat {other_address}")
        return store_pb2.RegisterSlaveResponse(success=True, state=self.data)
    
    # Mètode per mostrar els logs
    def log(self, msg):
        print(f"{colorama.Fore.YELLOW}[{self.id}]{colorama.Fore.RESET} {msg}")

# Mètode per registrar el Slave al Master
def register_to_node(other_address, my_address):
    channel = grpc.insecure_channel(other_address)
    stub = store_pb2_grpc.KeyValueStoreStub(channel)
    response = stub.registerNode(store_pb2.RegisterNodeRequest(address=my_address))
    if response.success:
        print(f"Node {my_address} registrat correctament al Node {other_address}")
        # Retornar l'estat del Master
        return response.state
    else:
        print(f"No s'ha pogut registrar el Node {my_address} al Node {other_address}")
        sys.exit(1)

# Mètode per iniciar el servidor gRPC
def serve(id, ip, port, weight, other_nodes):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    store_pb2_grpc.add_KeyValueStoreServicer_to_server(Node(id, weight), server)
    server.add_insecure_port(f"{ip}:{port}")
    server.start()
    print(f"Node escoltant al port {port}...")
    
    my_address = f"{ip}:{port}"
    
    for other in other_nodes:
        register_to_node(other, my_address)
        register_to_node(my_address, other)
    
    # Funció per gestionar les senyals SIGINT i SIGTERM
    def signal_handler(sig, frame):
        print(f"Parant {id}...")
        server.stop(0)
        sys.exit(0)

    # Assignar el gestor de senyals per SIGINT i SIGTERM
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        signal_handler(signal.SIGINT, None)

# Main
if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Ús: python3 node.py <id> <ip> <port> <weight> <ant_nodes>")
        sys.exit(1)
        
    id = sys.argv[1]
    ip = sys.argv[2]
    port = sys.argv[3]
    weight = sys.argv[4]
    ant_nodes = json.loads(sys.argv[5])
    serve(id, ip, port, weight, ant_nodes)
        
