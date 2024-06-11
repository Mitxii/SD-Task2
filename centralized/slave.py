import grpc
import time
import sys
import os
import signal
from concurrent import futures

# Obtenir path del directori actual
current_dir = os.path.dirname(__file__)

# Importar classes gRPC
proto_path = os.path.join(current_dir, "..", "proto")
sys.path.append(proto_path)
import store_pb2, store_pb2_grpc

# Importar altres classes
sys.path.append(current_dir)
from node import Node

# Classe Slave (filla de Node)
class Slave(Node):
    
    # Constructor
    def __init__(self, id, state):
        super().__init__(id)
        # L'estat inicial és l'obtingut al registrar-se al master per mantenir una consistència
        self.data = dict(state) 
    
    # Mètode per respondre a un 2PC
    def canCommit(self, request, context):
        self.log("2PC available")
        time.sleep(self.delay)
        return store_pb2.CommitResponse(can_commit=True)

# Mètode per registrar el Slave al Master
def register_to_master(master_address, slave_address):
    channel = grpc.insecure_channel(master_address)
    stub = store_pb2_grpc.KeyValueStoreStub(channel)
    response = stub.registerSlave(store_pb2.RegisterSlaveRequest(address=slave_address))
    if response.success:
        print(f"Slave {slave_address} registrat correctament al Master {master_address}")
        # Retornar l'estat del Master
        return response.state
    else:
        print(f"No s'ha pogut registrar el Slave {slave_address} al Master {master_address}")
        sys.exit(1)

# Mètode per iniciar el servidor gRPC
def serve(id, ip, port, master_address):
    slave_address = f"{ip}:{port}"
    state = register_to_master(master_address, slave_address)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    store_pb2_grpc.add_KeyValueStoreServicer_to_server(Slave(id, state), server)
    server.add_insecure_port(slave_address)
    server.start()
    print(f"Slave escoltant al port {port}...")
    
    # Funció per gestionar les senyals SIGINT i SIGTERM
    def signal_handler(sig, frame):
        print(f"Parant {id}...")
        server.stop(0)
        sys.exit(0)

    # Assignar el gestor de senyals per SIGINT i SIGTERM
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Bucle infinit
    while True:
        time.sleep(86400)

# Main
if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Ús: python3 slave.py <id> <ip> <port> <master_ip:master_port>")
        sys.exit(1)
        
    id = sys.argv[1]
    ip = sys.argv[2]
    port = sys.argv[3]
    master_address = sys.argv[4]
    serve(id, ip, port, master_address)