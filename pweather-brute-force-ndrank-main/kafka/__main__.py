import getopt
import logging
import os
import sys
from typing import Any, Dict, Final, Optional
import yaml, signal

from controller.controller import ControllerBase
from controller.kafka_controller import KafkaController
from dtos.consumer_message import NODE_ID
from service.low_resolution_service import LowResolutionService
from service.service import ServiceBase

logging.basicConfig(level=logging.DEBUG,format='message_queue-%(levelname)s:%(message)s')

properties_path: str = "properties.yaml"
properties: Dict

#set up for killing the server correctly
def signal_handler(signum: Any, frame: Any) -> None:
    """Handler to kill the server
    """
    global controller
    if not controller is None:
        controller.close()

controller: Optional[ControllerBase] = None
service: Optional[ServiceBase] = None

#---------------------PROCESS COMMAND LINE OPTIONS---------------------
opts, args = getopt.getopt(sys.argv[1:],"p:d:")

for opt in opts:
    logging.debug(opt)
    if opt[0] in ("-p"):
        properties_path = opt[1]

logging.debug("Path of properties file: " + properties_path)
#---------------------TAGS FROM PROPERTIES.YAML---------------------
#properties.yaml
NETWORK_CONFIG: Final = "network-config"
IP: Final = "ip"
PORT: Final = "port"

KAFKA: Final = "kafka"
GROUP_ID: Final = "group-id"
CLIENT_ID: Final = "client-id"

SERVICE: Final = "service"

NODE_IDS: Final = "node-ids"

#---------------------END OF TAGS FROM PROPERTIES.YAML---------------------
with open(properties_path, 'r') as f:
    properties = yaml.safe_load(f)

#---------------------RETRIEVE CONFIGURATION FROM YAML---------------------
#NETWORK CONFIGURATIONS
ip: str = properties[NETWORK_CONFIG][IP]
port: int = properties[NETWORK_CONFIG][PORT]

logging.info("Parsed properties: " + str(properties))

service = LowResolutionService(properties[NODE_IDS])
controller = \
    KafkaController(service, ip, port, properties[KAFKA][GROUP_ID], properties[KAFKA][CLIENT_ID])

signal.signal(signal.SIGINT, signal_handler)

controller.start()

logging.info("Server is dead")
os._exit(0)
