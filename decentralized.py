import subprocess
import sys
import signal
import yaml
import json

# Mètode per iniciar un Node
def start_node(id, ip, port, weight, ant_nodes):
    return subprocess.Popen(["python3", "decentralized/node.py", id, ip, str(port), str(weight), json.dumps(ant_nodes)])

# Mètode per obtenir les dades d'un fitxer .yaml
def load_config(config_file):
    with open(config_file, "r") as file:
        return yaml.safe_load(file)

# Funció per gestionar les senyals SIGINT i SIGTERM
def signal_handler(sig, frame):
    for node_process in node_processess:
        node_process.terminate()
    sys.exit(0)

# Main
if __name__ == "__main__":
    # Obtenir configuració del fitxer decentralized_config.yaml
    config = load_config("decentralized_config.yaml")
    
    ant_nodes = []
    
    # Nodes
    nodes = config["nodes"]
    node_processess = []
    for node in nodes:
        node_id = node["id"]
        node_ip = node["ip"]
        node_port = node["port"]
        node_weight = node["weight"]
        node_process = start_node(node_id, node_ip, node_port, node_weight, ant_nodes)
        ant_nodes.append(f"{node_ip}:{node_port}")
        node_processess.append(node_process)
        
    # Assignar el gestor de senyals per SIGINT i SIGTERM
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)    
        
    try:
        for node_process in node_processess:
            node_process.wait()
    except KeyboardInterrupt:
        signal_handler(signal.SIGINT, None)
    