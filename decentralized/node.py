import grpc
import threading
import sys
import os
import time
import colorama
import signal
import json
import pickle
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
    def __init__(self, id, weight, read_size, write_size, state):
        self.id = id
        self.weight = weight
        self.data = dict(state)
        self.delay = 0
        # Altres nodes pel quorum
        self.other_nodes = []
        # Valors per reads i writes
        self.read_size = read_size
        self.write_size = write_size
        # Variables pels backups
        if not os.path.exists("decentralized/states"):
            os.makedirs("decentralized/states")
        self.persistent_file = f"decentralized/states/{id}_data.pkl"
        self.load_state()
        
    # Mètode per guardar una dada
    def put(self, request, context):
        key = request.key
        value = request.value
        
        # Votació
        size = self.quorum(key)
        
        # Si s'arriba al pes necessari per escriure...
        if size >= self.write_size:
            self.log(f"Quorum succeded on PUT. Required={self.write_size}, Obtained={size}")
            for other_stub in self.other_nodes:
                # El doCommit s'executa en un thread per evitar delays dels Nodes
                threading.Thread(target=lambda: other_stub.doCommit(store_pb2.CommitRequest(key=key, value=value))).start()
            self.data[key] = value
            self.save_state()
            self.log(f"set key={key}, value={value}")
            time.sleep(self.delay)
            return store_pb2.PutResponse(success=True)
        else:
            self.log(f"Quorum failed on PUT. Required={self.write_size}, Obtained={size}")
            time.sleep(self.delay)
            return store_pb2.PutResponse(success=False)
    
    # Mètode per obtenir una dada
    def get(self, request, context):
        key = request.key
        
        # Votació
        size = self.quorum(key)
        
        # Si s'arriba al pes necessari per llegir...
        if size >= self.read_size:
            self.log(f"Quorum succeded on GET. Required={self.read_size}, Obtained={size}")
            value = self.data.get(key, "")
            found = key in self.data
            self.log(f"get key={key} -> value={value}")
            time.sleep(self.delay)
            return store_pb2.GetResponse(value=value, found=found)
        else:
            self.log(f"Quorum failed on GET. Required={self.read_size}, Obtained={size}")
            time.sleep(self.delay)
            return store_pb2.GetResponse(value="", found=False)
    
    # Mètode per implementar el protocol Quorum
    def quorum(self, key):
        self.log("Starting Quorum...")
        
        # Fase de votació
        size = 0
        for other_stub in self.other_nodes:
            response = other_stub.askVote(store_pb2.AskVoteRequest(key=key))
            size += response.weight
            
        return size + self.weight
    
    # Mètode per confirmar un Put
    def doCommit(self, request, context):
        key = request.key
        value = request.value
        self.data[key] = value
        self.save_state()
        self.log(f"set key={key}, value={value}")
        time.sleep(self.delay)
        return store_pb2.Empty()
    
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
    
    # Mètode per registrar un Node i passar-li l'estat actual
    def registerNode(self, request, context):
        other_address = request.address
        channel = grpc.insecure_channel(other_address)
        other_stub = store_pb2_grpc.KeyValueStoreStub(channel)
        self.other_nodes.append(other_stub)
        self.log(f"Node registrat {other_address}")
        return store_pb2.RegisterNodeResponse(success=True, state=self.data)
    
    # Mètode per enviar el pes durant un Quorum
    def askVote(self, request, context):
        time.sleep(self.delay)
        return store_pb2.AskVoteResponse(weight=self.weight)
    
    # Mètode per guardar l'estat actual
    def save_state(self):
        with open(self.persistent_file, 'wb') as f:
            pickle.dump(self.data, f)
    
    # Mètode per carregar l'estat anterior (en cas de tenir)
    def load_state(self):
        if os.path.exists(self.persistent_file):
            with open(self.persistent_file, 'rb') as f:
                self.data = pickle.load(f)
    
    # Mètode per mostrar els logs
    def log(self, msg):
        print(f"{colorama.Fore.YELLOW}[{self.id}]{colorama.Fore.RESET} {msg}")

# Mètode per registrar el Slave al Master
def register_to_node(other_address, my_address):
    while True:
        try:
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
        except grpc._channel._InactiveRpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                time.sleep(.5)
            else:
                raise e

# Mètode per iniciar el servidor gRPC
def serve(id, ip, port, weight, other_nodes, read_size, write_size):
    my_address = f"{ip}:{port}"
    state = {}
    
    # Recórrer tots els nodes que ja formen part del clúster
    for other in other_nodes:
        # Registrar el node actual al node iterat i obtenir el seu estat
        state = register_to_node(other, my_address)
        
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    store_pb2_grpc.add_KeyValueStoreServicer_to_server(Node(id, weight, read_size, write_size, state), server)
    server.add_insecure_port(f"{ip}:{port}")
    server.start()
    print(f"Node escoltant al port {port}...")
    
    # Recórrer tots els nodes que ja formen part del clúster
    for other in other_nodes:
        # Registrar el node iterat al node actual
        register_to_node(my_address, other)
    
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
    if len(sys.argv) != 8:
        print("Ús: python3 node.py <id> <ip> <port> <weight> <ant_nodes> <read_size> <write_size>")
        sys.exit(1)
        
    id = sys.argv[1]
    ip = sys.argv[2]
    port = sys.argv[3]
    weight = int(sys.argv[4])
    ant_nodes = json.loads(sys.argv[5])
    read_size = int(sys.argv[6])
    write_size = int(sys.argv[7])
    serve(id, ip, port, weight, ant_nodes, read_size, write_size)
        
