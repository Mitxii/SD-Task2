import subprocess
import sys
import signal
import time
import yaml

# Mètode per iniciar un Master
def start_master(id, ip, port):
    return subprocess.Popen(["python3", "centralized/master.py", id, ip, str(port)])

# Mètode per iniciar un Slave
def start_slave(id, ip, port, master_ip, master_port):
    return subprocess.Popen(["python3", "centralized/slave.py", id, ip, str(port), f"{master_ip}:{master_port}"])

# Mètode per obtenir les dades d'un fitxer .yaml
def load_config(config_file):
    with open(config_file, "r") as file:
        return yaml.safe_load(file)

# Funció per gestionar la senyal SIGINT
def signal_handler_INT(sig, frame):
    sys.exit(0)
    
# Funció per gestionar la senyal SIGTERM
def signal_handler_TERM(sig, frame):
    for process in processess:
        process.terminate()
    sys.exit(0)

# Main
if __name__ == "__main__":
    # Obtenir configuració del fitxer centralized_config.yaml
    config = load_config("centralized_config.yaml")
    
    # Processos de tots els nodes
    processess = []
    
    # Master
    master_ip = config["master"]["ip"]
    master_port = config["master"]["port"]
    process = start_master("master", master_ip, master_port)
    processess.append(process)
    time.sleep(1)
    
    # Slaves
    slaves = config["slaves"]
    for slave in slaves:
        slave_id = slave["id"]
        slave_ip = slave["ip"]
        slave_port = slave["port"]
        process = start_slave(slave_id, slave_ip, slave_port, master_ip, master_port)
        processess.append(process)
        
    # Assignar el gestor de senyals per SIGINT i SIGTERM
    signal.signal(signal.SIGINT, signal_handler_INT)
    signal.signal(signal.SIGTERM, signal_handler_TERM)    
    
    # Bucle infinit
    while True:
        time.sleep(86400)
    