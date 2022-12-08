import csv
from datetime import datetime
import getopt
import sys
from time import time_ns
import traceback
from typing import Any, Dict, Final, List, Optional, TextIO
import yaml, logging

from node_client.client import ClientModule, SearchRequestDto, SearchResultDto, worker_node

logging.basicConfig(level=logging.DEBUG,format='master_node-%(levelname)s:%(message)s')

RESULT_PATH: str
properties: Dict
requests_file: Dict
nodes: List[worker_node] = []
requests: List[SearchRequestDto] = []

properties_path: str = "properties.yaml"
requests_path: str = "requests.yaml"

#---------------------PROCESS COMMAND LINE OPTIONS---------------------
opts, args = getopt.getopt(sys.argv[1:],"p:r:")

for opt in opts:
    logging.debug(opt)
    if opt[0] in ("-p"):
        properties_path = opt[1]
    elif opt[0] in ("-r"):
        requests_path = opt[1]

logging.debug("Using properties file: " + properties_path)
logging.debug("Using requests file: " + requests_path)

#---------------------TAGS FROM PROPERTIES.YAML---------------------
#properties.yaml
RESULTS_PATH: Final = "results-path"
NODE_PROPERTIES: Final = "node-properties"
IP: Final = "ip"
PORT: Final = "port"
DATASET_START_DATE: Final = "dataset_start_date"
DATASET_END_DATE: Final = "dataset_end_date"
YEAR: Final = "year"
MONTH: Final = "month"
DAY: Final = "day"
HOUR: Final = "hour"

#requests.yaml
REQUESTS: Final = "requests"
REQUEST_NAME: Final = "request-name"
NUM_OF_RESULTS: Final = "number-of-results"
INPUT_PATH: Final = "input-path"
TIME_INSTANCES: Final = "time-instances"
OPTIONS: Final = "options"
CORRELATION_FUNCTION: Final = "correlation-function"

#option tags in data vars

#---------------------END OF TAGS FROM PROPERTIES.YAML---------------------
#AUXILIAR METHODS
def extract_date(arg: Any) -> datetime:
    return datetime(arg[YEAR], arg[MONTH], arg[DAY], arg[HOUR])

with open(properties_path, 'r') as f:
    properties = yaml.safe_load(f)

with open(requests_path, 'r') as f:
    requests_file = yaml.safe_load(f)

#---------------------LOAD GENERAL PROPERTIES---------------------
RESULT_PATH = properties[RESULTS_PATH]
if RESULT_PATH[-1] != '/':
    RESULT_PATH += '/'

#---------------------LOAD NODES---------------------
logging.debug("Loading worker nodes:")
if properties[NODE_PROPERTIES] is None:
    nodes = []
else:
    for node in properties[NODE_PROPERTIES]:
        new_node: worker_node = worker_node(node[IP], node[PORT]) 
        nodes.append(new_node)
        logging.debug(new_node)

#---------------------LOAD REQUESTS---------------------
logging.debug("Loading requests")
for request in requests_file[REQUESTS]:
    logging.debug(request)
    new_request: SearchRequestDto = \
        SearchRequestDto(request[REQUEST_NAME] + "-" + datetime.now().strftime("%H-%M-%S_%Y-%m-%d"), 
                         request[NUM_OF_RESULTS] if NUM_OF_RESULTS in request else None,
                         request[INPUT_PATH],
                         request[TIME_INSTANCES],
                         extract_date(properties[DATASET_START_DATE]),
                         extract_date(properties[DATASET_END_DATE]),
                         request[OPTIONS],
                         request[OPTIONS][CORRELATION_FUNCTION])
    requests.append(new_request)
    logging.debug(new_request)

#---------------------REQUEST EXECUTION PROCESS---------------------
client_comm: ClientModule = ClientModule(nodes)

result_file_name: str = RESULT_PATH + "results_" +\
                        datetime.now().strftime("%H:%M:%S_%Y-%m-%d") + ".res"

result_file_pointer: TextIO = open(result_file_name, mode='w')

REQUEST_SEPARATOR: Final = "-" * 100
REQUEST_HEADER: Final = "Request "
REQUEST_INPUT: Final = "Provided Input:\n"

REQUEST_RESULT: Final = "Result:\n"
EXEC_TIME: Final = "Execution time in nanoseconds: "
EXEC_RESULT: Final = "Obtained results: \n"

REQUEST_ERROR: Final = "Error occured:\n"
EXCEPTION_NAME: Final = "Exception name: "
EXCEPTION_TRACE: Final = "Exception traceback: \n"

logging.debug(REQUEST_SEPARATOR)
logging.debug("Starting requests")

"""Section that executes all the requests written into the requests file
and writes down the results in csv files and in a single .res file
"""
for request in requests:
    result_file_pointer.write(REQUEST_HEADER + request.request_name + "\n\n")
    result_file_pointer.write(REQUEST_INPUT)
    result_file_pointer.write(str(request) + "\n\n")

    try:
        result: SearchResultDto
        start_time: int = time_ns()

        result = client_comm.search_request(request)
        logging.debug(result)
        end_time: int = time_ns()

        result_file_pointer.write(REQUEST_RESULT)
        result_file_pointer.write(EXEC_TIME + str(end_time - start_time) + "\n")
        result_file_pointer.write(EXEC_RESULT + str(result) + "\n")

        with open(RESULT_PATH + request.request_name + ".csv", "w", encoding="UTF8") as f:
            writer = csv.writer(f)
            header: List[str]
            values: List[List[Any]]
            header, values = result.list_struct_to_csv()

            writer.writerow(header)
            writer.writerows(values)

        logging.debug("Request " + request.request_name + " finished with success")

    except Exception as e:
        result_file_pointer.write(REQUEST_ERROR)
        result_file_pointer.write(EXCEPTION_NAME + type(e).__name__ + "\n")
        result_file_pointer.write(EXCEPTION_TRACE + traceback.format_exc() + "\n")
        logging.warning("Request " + request.request_name + " finished with an exception")
        logging.debug(traceback.format_exc())
    result_file_pointer.write(REQUEST_SEPARATOR + "\n")

result_file_pointer.close()