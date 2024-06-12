import grpc
import time
import sys
import os
import signal
import pickle
import threading
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

# Classe Master (filla de Node)
class Master(Node):
    
    # Constructor
    def __init__(self, id):
        super().__init__(id)
        # Slaves registrats
        self.slaves = []
        # Fitxer de backup en cas de fallada
        self.persistent_file = f"centralized/{id}_data.pkl"
        self.load_state()
        
    # Mètode per guardar una dada
    def put(self, request, context):
        key = request.key
        value = request.value
        
        # Protocol 2PC
        success = self.two_phase_commit(key, value)
        if success:
            # Fer escriptura
            self.write_value(key,value)
            # Guardar estat després de l'operació
            self.save_state()
            self.log(f"set key={key}, value={value}")
        else:
            self.log("2PC failed. Aborting...")
            
        time.sleep(self.delay)
        return store_pb2.PutResponse(success=success)
    
    # Mètode per implementar el protocol 2PC
    def two_phase_commit(self, key, value):
        self.log("Starting 2PC...")
        can_commit = True
        
        # Fase 1 (votació)
        i = 1
        for slave_stub in self.slaves:
            self.log(f"2PC request {i}")
            i += 1
            try:
                response = slave_stub.canCommit(store_pb2.CommitRequest(key=key, value=value))
                if not response.can_commit:
                    can_commit = False
                    break
            except grpc.RpcError as e:
                self.log(f"error connecting to slave. Removing from registered ones...")
                self.slaves.remove(slave_stub)
        
        # Fase 2 (decisió)
        if can_commit:
            for slave_stub in self.slaves:
                # El doCommit s'executa en un thread per evitar delays dels Slaves
                threading.Thread(target=lambda: slave_stub.doCommit(store_pb2.CommitRequest(key=key, value=value))).start()

        return can_commit        
    
    # Mètode per guardar l'estat actual
    def save_state(self):
        with open(self.persistent_file, 'wb') as f:
            pickle.dump(self.data, f)
    
    # Mètode per carregar l'estat guardat
    def load_state(self):
        if os.path.exists(self.persistent_file):
            with open(self.persistent_file, 'rb') as f:
                self.data = pickle.load(f)
        else:
            self.data = {}
    
    # Mètode per registrar un Slave i passar-li l'estat actual
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
    
    # Bucle infinit
    while True:
        time.sleep(86400)

# Main
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Ús: python3 master.py <id> <ip> <port>")
        sys.exit(1)
        
    id = sys.argv[1]
    ip = sys.argv[2]
    port = sys.argv[3]
    serve(id, ip, port)