import grpc
import time
import sys
import os
from concurrent import futures

current_dir = os.path.dirname(__file__)

# Importar classes gRPC
proto_path = os.path.join(current_dir, "..", "proto")
sys.path.append(proto_path)
import store_pb2
import store_pb2_grpc

# Importar altres classes
sys.path.append(current_dir)
from node import Node

# Classe Master (filla de Node)
class Master(Node):
    
    def __init__(self):
        super().__init__()
        self.slaves = []
        
    def put(self, request, context):
        key = request.key
        value = request.value
        
        # Protocol 2PC
        print("2PC")
        if self.two_phase_commit(key, value):
            self.data[key] = value
            return store_pb2.PutResponse(success=True)
        else:
            return store_pb2.PutResponse(success=False)
        
    def two_phase_commit(self, key, value):
        can_commit = True
        
        for slave_stub in self.slaves:
            response = slave_stub.canCommit(store_pb2.CommitRequest(key=key, value=value))
            if not response.can_commit:
                can_commit = False
                break
            
        if can_commit:
            for slave_stub in self.slaves:
                slave_stub.doCommit(store_pb2.CommitRequest(key=key, value=value))
            return True
        else:
            return False
        
    def registerSlave(self, request, context):
        slave_address = request.address
        channel = grpc.insecure_channel(slave_address)
        slave_stub = store_pb2_grpc.KeyValueStoreStub(channel)
        self.slaves.append(slave_stub)
        return store_pb2.RegisterSlaveResponse(success=True)

# Mètode per iniciar el servidor gRPC
def serve(ip, port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    store_pb2_grpc.add_KeyValueStoreServicer_to_server(Master(), server)
    server.add_insecure_port(f"{ip}:{port}")
    server.start()
    print(f"Master escoltant al port {port}...")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

# Main
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Ús: python3 master.py <ip> <port>")
        sys.exit(1)
        
    ip = sys.argv[1]
    port = sys.argv[2]
    serve(ip, port)