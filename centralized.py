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

# Funció per gestionar les senyals SIGINT i SIGTERM
def signal_handler(sig, frame):
    print(f'Signal {sig} received. Terminating processes gracefully.')
    master_process.terminate()
    for slave_process in slave_processess:
        slave_process.terminate()
    sys.exit(0)

# Main
if __name__ == "__main__":
    # Obtenir configuració del fitxer centralized_config.yaml
    config = load_config("centralized_config.yaml")
    
    # Master
    master_ip = config["master"]["ip"]
    master_port = config["master"]["port"]
    master_process = start_master("master", master_ip, master_port)
    time.sleep(1)
    
    # Slaves
    slaves = config["slaves"]
    slave_processess = []
    for slave in slaves:
        slave_id = slave["id"]
        slave_ip = slave["ip"]
        slave_port = slave["port"]
        slave_process = start_slave(slave_id, slave_ip, slave_port, master_ip, master_port)
        slave_processess.append(slave_process)
        
    # Assignar el gestor de senyals per SIGINT i SIGTERM
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)    
        
    try:
        master_process.wait()
        for slave_process in slave_processess:
            slave_process.wait()
    except KeyboardInterrupt:
        signal_handler(signal.SIGINT, None)
    