import grpc
import time
import sys
import os
import signal
import pickle
from concurrent import futures

current_dir = os.path.dirname(__file__)

# Importar classes gRPC
proto_path = os.path.join(current_dir, "..", "proto")
sys.path.append(proto_path)
import store_pb2, store_pb2_grpc

# Importar altres classes
sys.path.append(current_dir)
from node import Node

# Classe Master (filla de Node)
class Master(Node):
    
    def __init__(self, id):
        super().__init__(id)
        # Slaves registrats
        self.slaves = []
        # Fitxer de backup en cas de fallada
        self.persistent_file = f"centralized/{id}_data.pkl"
        self.load_state()
        
    def put(self, request, context):
        key = request.key
        value = request.value
        
        # Protocol 2PC
        if self.two_phase_commit(key, value):
            self.data[key] = value
            self.save_state()                               # Desar l'estat després de l'operació
            self.log(f"set key={key}, value={value}")
            time.sleep(self.delay)
            return store_pb2.PutResponse(success=True)
        else:
            self.log("2PC failed")
            time.sleep(self.delay)
            return store_pb2.PutResponse(success=False)
        
    def two_phase_commit(self, key, value):
        can_commit = True
        
        self.log("Starting 2PC...")
        i = 1
        for slave_stub in self.slaves:
            self.log(f"2PC request {i}")
            i += 1
            response = slave_stub.canCommit(store_pb2.CommitRequest(key=key, value=value))
            if not response.can_commit:
                can_commit = False
                break
            
        if can_commit:
            for slave_stub in self.slaves:
                slave_stub.doCommit(store_pb2.CommitRequest(key=key, value=value))
            return True
        
    def save_state(self):
        with open(self.persistent_file, 'wb') as f:
            pickle.dump(self.data, f)
    
    def load_state(self):
        if os.path.exists(self.persistent_file):
            with open(self.persistent_file, 'rb') as f:
                self.data = pickle.load(f)
        else:
            self.data = {}
        
    def registerSlave(self, request, context):
        slave_address = request.address
        channel = grpc.insecure_channel(slave_address)
        slave_stub = store_pb2_grpc.KeyValueStoreStub(channel)
        self.slaves.append(slave_stub)
        self.log(f"Slave registrat {slave_address}")
        return store_pb2.RegisterSlaveResponse(success=True, state=self.data)

# Mètode per iniciar el servidor gRPC
def serve(id, ip, port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    store_pb2_grpc.add_KeyValueStoreServicer_to_server(Master(id), server)
    server.add_insecure_port(f"{ip}:{port}")
    server.start()
    print(f"Master escoltant al port {port}...")
    
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
    if len(sys.argv) != 4:
        print("Ús: python3 master.py <id> <ip> <port>")
        sys.exit(1)
        
    id = sys.argv[1]
    ip = sys.argv[2]
    port = sys.argv[3]
    serve(id, ip, port)