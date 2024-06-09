import grpc
import time
import sys
import os
from concurrent import futures

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
    
    def __init__(self, id):
        super().__init__(id)
        self.action = ""
        
    def canCommit(self, request, context):
        time.sleep(self.delay)
        self.log("2PC available")
        return store_pb2.CommitResponse(can_commit=True)

# Mètode per registrar el Slave al Master
def register_to_master(master_address, slave_address):
    channel = grpc.insecure_channel(master_address)
    stub = store_pb2_grpc.KeyValueStoreStub(channel)
    response = stub.registerSlave(store_pb2.RegisterSlaveRequest(address=slave_address))
    if response.success:
        print(f"Slave {slave_address} registrat correctament al Master {master_address}")
    else:
        print(f"No s'ha pogut registrar el Slave {slave_address} al Master {master_address}")

# Mètode per iniciar el servidor gRPC
def serve(id, ip, port, master_address):
    slave_address = f"{ip}:{port}"
    register_to_master(master_address, slave_address)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    store_pb2_grpc.add_KeyValueStoreServicer_to_server(Slave(id), server)
    server.add_insecure_port(slave_address)
    server.start()
    print(f"Slave escoltant al port {port}...")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

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