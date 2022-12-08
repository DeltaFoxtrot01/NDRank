from auxiliar.component_injector import component_injector
import getopt
import logging
import os
import sys
from typing import Any, Dict, Final, List, Optional
from auxiliar.ts_logger import set_debug_logger_as_debug
from protocol import protocol_pb2_grpc
from controller import brute_force_controller
import grpc, concurrent, yaml, signal, numpy as np, warnings

from repository.repository_layer import RepositoryLayer
from service.service_main_structure import ServiceLayer
from controller import ndrank_controller

logging.basicConfig(level=logging.DEBUG,format='worker_node-%(levelname)s:%(message)s')
np.seterr(all='raise')
warnings.filterwarnings("error", category=RuntimeWarning)

properties_path: str = "properties.yaml"
properties: Dict

server_obj: Optional[grpc.Server] = None
repositories: List[RepositoryLayer] = []
service: ServiceLayer

low_res_repositories: List[RepositoryLayer] = []
low_res_service: ServiceLayer

delete_request_input: bool = True
node_id: str

#set up for killing the server correctly
def signal_handler(signum: Any, frame: Any) -> None:
    """Handler to kill the server
    """
    global server_obj
    
    if not server_obj is None:
        server_obj.stop(0)
        logging.info("Killing server")

#---------------------PROCESS COMMAND LINE OPTIONS---------------------
opts, args = getopt.getopt(sys.argv[1:],"p:d:")

for opt in opts:
    logging.debug(opt)
    if opt[0] in ("-p"):
        properties_path = opt[1]
    elif opt[0] in ("-d"):
        delete_request_input = not opt[1].lower() == "false"

logging.info("Path of properties file: " + properties_path)
#---------------------TAGS FROM PROPERTIES.YAML---------------------
#properties.yaml
NODE_ID: Final = "node-id"
NETWORK_CONFIG: Final = "network-config"
IP: Final = "ip"
PORT: Final = "port"
KAFKA_IP: Final = "kafka-ip"
KAFKA_PORT: Final = "kafka-port"

AVAILABLE_PORTS: Final = "available_ports"
FROM: Final = "from"
TO: Final = "to"

TEMPORARY_FOLDER: Final = "temporary-folder"

CONTROLLER: Final = "controller"
CONTROLLER_BRUTE_FORCE: Final = "brute-force"
CONTROLLER_NDRANK: Final = "ndrank"

SERVICE: Final = "service"
LOW_RES_SERVICE: Final = "low-resolution-service"

REPOSITORY: Final = "repository"
LOW_RES_REPOSITORY: Final = "low-resolution-repository"
TYPE: Final = "type"
PATHS: Final = "paths"

DEBUG_TS_LOG: Final = "debug-ts-log"
#---------------------END OF TAGS FROM PROPERTIES.YAML---------------------
with open(properties_path, 'r') as f:
    properties = yaml.safe_load(f)

#---------------------RETRIEVE CONFIGURATION FROM YAML---------------------
#DEBUG LOG CONFIGURATION
if DEBUG_TS_LOG in properties and properties[DEBUG_TS_LOG] == True:
    set_debug_logger_as_debug()


#NETWORK AND NODE CONFIGURATIONS
ip: str = properties[NETWORK_CONFIG][IP]
port: int = properties[NETWORK_CONFIG][PORT]

from_port: int = properties[NETWORK_CONFIG][AVAILABLE_PORTS][FROM]
to_port: int = properties[NETWORK_CONFIG][AVAILABLE_PORTS][TO]

#LOCAL FILE CONFIGURATIONS
temporary_folder_path: str = properties[TEMPORARY_FOLDER]

logging.info("Parsed properties: " + str(properties))
logging.info("Should received input files be deleted? -> " + str(delete_request_input))

for path in properties[REPOSITORY][PATHS]:
    repositories.append(
        component_injector.get_repo_instance(properties[REPOSITORY][TYPE],
                                             path,"settings.yaml"))
if LOW_RES_REPOSITORY in properties:
    for path in properties[LOW_RES_REPOSITORY][PATHS]:
        low_res_repositories.append(
            component_injector.get_repo_instance(properties[LOW_RES_REPOSITORY][TYPE],
                                                 path,"settings.yaml"))

service = \
    component_injector.get_service_instance(properties[SERVICE], repositories)
if LOW_RES_REPOSITORY in properties:
    low_res_service = \
        component_injector.get_service_instance(properties[LOW_RES_SERVICE], low_res_repositories)

#gRPC CONFIGURATION
server_obj = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))

address: str = ip + ":" + str(port)

kafka_ip: str
kafka_port: int

brute_force_controller_inst: Optional[brute_force_controller.BruteForceController] = None
ndrank_conttroller_inst: Optional[ndrank_controller.NdrankController] = None

#CONTROLLER CONFIGURATION
"""
Three controllers are provided:
    - the brute force controller
    - the ndrank controller

These represent all kind of workers both the brute force architecture and the
ndrank can have. The selection of the controller will determine how the role the
worker node will have
"""
if properties[CONTROLLER] == CONTROLLER_BRUTE_FORCE:
    brute_force_controller_inst = \
        brute_force_controller.BruteForceController(
            ip,from_port,to_port, temporary_folder_path, 
            service, delete_request_input)
    
    protocol_pb2_grpc.add_ControllerServiceServicer_to_server(
        brute_force_controller_inst, server_obj)

elif properties[CONTROLLER] == CONTROLLER_NDRANK:
    node_id = properties[NODE_ID]
    kafka_ip = properties[NETWORK_CONFIG][KAFKA_IP]
    kafka_port = properties[NETWORK_CONFIG][KAFKA_PORT]

    ndrank_conttroller_inst = \
        ndrank_controller.NdrankController(kafka_ip, kafka_port,
            node_id, ip, from_port, to_port, temporary_folder_path,
            low_res_service, service, delete_request_input)

    protocol_pb2_grpc.add_ControllerServiceServicer_to_server(
        ndrank_conttroller_inst, server_obj)

else:
    raise ValueError("Invalid type for controller " + properties[CONTROLLER])

signal.signal(signal.SIGINT, signal_handler)

#SERVER START
server_obj.add_insecure_port(address)
logging.info("Started server at address " + address)
server_obj.start()
server_obj.wait_for_termination()
logging.info("Server is dead")
os._exit(0)
