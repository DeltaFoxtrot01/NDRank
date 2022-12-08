import logging
import traceback
from typing import Dict, Iterator, List, Tuple
import grpc
from auxiliar.ts_logger import get_ts_debug_handler
from controller.auxiliar.request_parameters_factory import list_of_files_factory, request_parameters_factory, separate_files_by_data_vars
from controller.file_protocol.dtos.dataset_properties import InputFileProperties, factory_InputFileProperties
from controller.file_protocol.file_protocol import FileProtocol, FileTransferInstance, factory_FilePortMapping
from protocol import protocol_pb2_grpc
from protocol.protocol_pb2 import AnalogueResponse, SearchRequest, SearchResponse
from service.data_types import ResultContainer
from service.service_main_structure import RequestParameters, ServiceLayer
from correlation_functions.main_structure import CorrelationFunction
from auxiliar.component_injector import component_injector


class BruteForceController(protocol_pb2_grpc.ControllerServiceServicer):
    """
    Responsible for the protocol section of the worker.

    Deals with receiving requests and transfering the input files.

    Also calls the service layer in order to start the search process
    """

    def __init__(self, ip: str, from_port:int, to_port:int, temp_folder_path: str, service: ServiceLayer, delete_input_files: bool = True) -> None:
        """Basic constructor for controller layer.

        A limit for the ports that can by the file protocol is defined.

        Args:
            ip (str): ip address to be used for socket comm.
            socket_port (int): port for main socket communication
            from_port (int): start limit port for file protocol
            to_port (int): end limit port for file protocol
            temp_folder_path (str): path of folders to store temporary files

        Raises:
            ValueError: if the to_port is lower or equal to from_port
        """
        super().__init__()
        self._from_port: int = from_port
        self._to_port: int = to_port
        self._ip: str = ip
        self._temporary_folder_path: str = temp_folder_path
        self._service: ServiceLayer = service
        self._file_protocol: FileProtocol = \
            FileProtocol(ip, from_port, to_port, temp_folder_path)
        self._delete_input_file: bool = delete_input_files

    def search_analogues(self, request: SearchRequest, context: grpc.ServicerContext) -> Iterator[SearchResponse]:
        """
        Receives request from master node.

        Worker uses streaming response in order to provide the socket port mapping for each file and
        the final response.

        This happens in two different messages.

        Args:
            request (SearchRequest): grpc request
            context (grpc.ServicerContext): grpc context object

        Yields:
            Iterator[SearchResponse]: streaming channel for worker node
        """
        try:
            debug_ts_logger: logging.Logger = get_ts_debug_handler()

            #processes the arguments received via gRPC
            corr_function: CorrelationFunction = \
                component_injector.get_correlation_function_instance(request.correlation_function)

            logging.info("Received a new request with id" + request.request_id + ", mapping ports")
            request_parameters: RequestParameters = request_parameters_factory(request.options)
            #dataset_files: List[InputFileProperties] = \
            #    list(map(lambda obj: factory_InputFileProperties(obj),
            #             request.input_files))

            #start communication process to transfer the input files
            dataset_files: List[InputFileProperties] = list_of_files_factory(request.input_files)
        
            file_transfer_obj: FileTransferInstance = self._file_protocol.open_sockets(dataset_files)

            logging.info("Executing port mapping for request id " + request.request_id)
            logging.debug(request)
            report_mapping_response: SearchResponse = SearchResponse()
            report_mapping_response.mappings.extend(
                        map(lambda obj: factory_FilePortMapping(obj),
                            file_transfer_obj.port_mapping))

            yield report_mapping_response

            response: SearchResponse = SearchResponse()
        
            created_files: List[Tuple[str,InputFileProperties]] = \
                self._file_protocol.wait_for_files_to_transfer(file_transfer_obj)
            
            logging.info("Files have been transfered, executing search for request id " + request.request_id)
            
            #execute the search with the service
            debug_ts_logger.debug("STARTING SEARCH ON DATASET")
            results: Dict[str, ResultContainer] = \
                self._service.execute_search(separate_files_by_data_vars(created_files),request_parameters, corr_function,request.number_of_results)[0]
            debug_ts_logger.debug("ENDED SEARCH ON DATASET")
            
            logging.info("Search finished, sending results of request id " + request.request_id)

            for ts in results:
                analogue: AnalogueResponse = AnalogueResponse()
                analogue.timestamp = ts
                analogue.similarity_value = results[ts].value
                analogue.time_instances = results[ts].sum_counter
                response.analogues.append(analogue)

            response.reverse_sort_order_corr_function = corr_function.is_reverse_order()
            yield response

            if self._delete_input_file:
                file_transfer_obj.delete_files()

            logging.info("Search finished for request id " + request.request_id)

        except Exception as e:
            logging.exception(e)
            try:                
                file_transfer_obj.delete_files()
            except NameError:
                pass

            try:                
                self._file_protocol.release_ports(file_transfer_obj)
            except NameError:
                pass

            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(traceback.format_exc())