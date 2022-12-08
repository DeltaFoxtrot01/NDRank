import logging, subprocess, traceback, xarray, grpc
import numpy as np
from typing import Dict, Iterator, List, Optional, Tuple
from controller.auxiliar.request_parameters_factory import list_of_files_factory, request_parameters_factory, separate_files_by_data_vars
from controller.file_protocol.dtos.dataset_properties import InputFileProperties, factory_InputFileProperties
from controller.file_protocol.file_protocol import FileProtocol, FileTransferInstance, factory_FilePortMapping
from controller.kafka_protocol.kafka_protocol import KafkaProtocol
from protocol import protocol_pb2_grpc
from protocol.protocol_pb2 import AnalogueResponse, SearchRequest, SearchResponse
from repository.repository_layer import RepositoryMetadata
from service.data_types import ResultContainer
from service.service_main_structure import RequestParameters, ServiceLayer
from auxiliar.xarray_aux import open_dataset_with_file_name
from correlation_functions.main_structure import CorrelationFunction
from auxiliar.component_injector import component_injector

class NdrankController(protocol_pb2_grpc.ControllerServiceServicer):
    """
    Responsible for the protocol section of the worker.

    Deals with receiving requests and transfering the input files.

    Also calls the service layer in order to start the search process
    """

    def __init__(self, kafka_host: str, kafka_port: int, node_id:str, ip: str, from_port:int, 
                 to_port:int, temp_folder_path: str, low_resolution_service: ServiceLayer,
                 full_resolution_service: ServiceLayer, delete_input_files: bool = True) -> None:
        """Basic constructor for controller layer.

        A limit for the ports that can by the file protocol is defined.

        Args:            
            kafka_host (str): ip of kafka server
            kafka_port (int): port of kafka server
            ip (str): ip address to be used for socket comm.
            socket_port (int): port for main socket communication
            from_port (int): start limit port for file protocol
            to_port (int): end limit port for file protocol
            temp_folder_path (str): path of folders to store temporary files
            low_resolution_service (ServiceLayer): component responsible for searching 
            in the local partition of the low resolution dataset
            full_resolution_service (ServiceLayer): component responsible for searching 
            in the local partition of the full resolution dataset
            delete_input_files (bool): if received input files should be deleted or not

        Raises:
            ValueError: if the to_port is lower or equal to from_port
        """
        super().__init__()
        self._from_port: int = from_port
        self._to_port: int = to_port
        self._ip: str = ip
        self._temporary_folder_path: str = temp_folder_path
        self._low_resolution_service: ServiceLayer = low_resolution_service
        self._full_resolution_service: ServiceLayer = full_resolution_service
        self._file_protocol: FileProtocol = \
            FileProtocol(ip, from_port, to_port, temp_folder_path)
        self._delete_input_file: bool = delete_input_files

        self._node_id: str = node_id
        self._kafka_protocol: KafkaProtocol = \
            KafkaProtocol(kafka_host, kafka_port)

    def _reduce_resolution_single_data_var(self, received_files: List[str], data_var: str) -> List[str]:
        """Reduces the resolution of the received files

        Args:
            received_files (List[str]): path of the files received
            data_var (str): data variable being analysed

        Returns:
            List[str]: list of the newly created files
        """
        res: List[str] = []
        aux_ds: List[Tuple[xarray.Dataset,str,np.datetime64]] = []
        time_coord: Optional[str] = None
        repo_meta: RepositoryMetadata = \
            self._full_resolution_service.repositories.get_metadata_by_data_var(data_var)
        resolution_parameters = self._low_resolution_service.get_low_resolution_parameters(data_var)

        for file in received_files:
            dataset: xarray.Dataset = open_dataset_with_file_name(file)
            new_dataset_name: str = self._temporary_folder_path + "/" +  "temp_reduced_" + file.split("/")[-1]
            logging.debug("Created reduced file: " + new_dataset_name)
            for coord in resolution_parameters:
                if coord in ("step", "time"):
                    time_coord = coord
                    continue
                dataset = dataset.coarsen({
                                    coord: resolution_parameters[coord]
                                 },boundary="trim")\
                                 .mean()
            datetime_input: np.datetime64 = \
                dataset.coords[repo_meta.time_variation_dim].values + \
                dataset.coords[repo_meta.time_initial_dim].values
            aux_ds.append((dataset, new_dataset_name, datetime_input))
            
        if not time_coord is None:
            aux_ds.sort(key=lambda elem: elem[2]) # type: ignore
            final_aux: List[Tuple[xarray.Dataset,str,np.datetime64]] = []

            for i in range(0,len(aux_ds) - len(aux_ds) % resolution_parameters[time_coord], 
                    resolution_parameters[time_coord]):
                ds_ls: List[xarray.Dataset] = []
                for j in range(i, i + resolution_parameters[time_coord]):
                    ds_ls.append(aux_ds[j][0])
                    
                ds_merged: xarray.Dataset = xarray.concat(ds_ls, time_coord)
                ds_merged = ds_merged.coarsen(
                                {
                                    time_coord: resolution_parameters[time_coord]
                                },boundary="trim")\
                                .mean()
                final_aux.append((ds_merged, aux_ds[i][1], aux_ds[i][2]))

            aux_ds = final_aux

        for pair in aux_ds:
            pair[0].to_netcdf(pair[1])
            pair[0].close()
            res.append(pair[1])
            
        return res

    def _reduce_resolution(self, received_files: Dict[str,List[str]]) -> Dict[str,List[str]]:
        res: Dict[str,List[str]] = {}

        for data_var in received_files:
            res[data_var] = self._reduce_resolution_single_data_var(received_files[data_var],data_var)

        return res

    def _process_candidates(self, service: ServiceLayer, input: Dict[str,List[str]], 
        request: SearchRequest, request_parameters: RequestParameters, 
        corr_function: CorrelationFunction) -> Tuple[Dict[str, ResultContainer], int]:
        """Executes the search for candidates and returns the obtained results

        Args:
            service (ServiceLayer): service being used
            input (Dict[str,List[str]]): list of file paths for the input
            request (SearchRequest): request object received
            request_parameters (RequestParameters): request parameters
            corr_function (CorrelationFunction): used correlation function

        Returns:
            Tuple[Dict[str, ResultContainer], int]: final results after the list of 
            candidates has been used
        """
        logging.debug("Searching for candidates")
        candidates, input_size = \
            service.execute_search_for_candidates(
                    input, request_parameters, 
                    corr_function, request.number_of_results)
        
        candidates_for_kafka: Dict = \
            self._kafka_protocol.convert_candidates_to_kafka_object(
                candidates, input_size,
                request.number_of_results, 
                corr_function.is_reverse_order())
        self._kafka_protocol.submit_candidates(
            request.request_id, candidates_for_kafka, self._node_id)

        return service.execute_search_on_ts(
                self._kafka_protocol.get_candidates_kafka(request), 
                input, request_parameters, corr_function, request.number_of_results)

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
            #processes the attributes received by gRPC
            corr_function: CorrelationFunction = \
                component_injector.get_correlation_function_instance(request.correlation_function)

            original_input: List[Tuple[str,InputFileProperties]]
            original_input_organized: Dict[str,List[str]]
            low_res_input: Dict[str,List[str]]
            results: Dict[str, ResultContainer]
            input_size: int
            logging.info("Received a new request with id: " + request.request_id + ", mapping ports")
            logging.debug(request)
            request_parameters: RequestParameters = request_parameters_factory(request.options)
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
        
            original_input = self._file_protocol.wait_for_files_to_transfer(file_transfer_obj)
            original_input_organized = separate_files_by_data_vars(original_input)
            logging.info("Files have been transfered, executing search for request id " + request.request_id)
            logging.info("Reducing the resolution of the input")
            logging.debug(original_input)
            low_res_input = self._reduce_resolution(original_input_organized)

            #There may be scenarios where a low resolution service is not available by choice
            #but it is usefull to use the connection with the queue for other purposes like the
            #list of candidates
            if self._low_resolution_service is None:
                if self._full_resolution_service.uses_global_candidates:
                    results, input_size = \
                        self._process_candidates(
                            self._full_resolution_service,original_input,
                            request, request_parameters, corr_function)
                else:
                    results, input_size = \
                        self._full_resolution_service.execute_search(
                            original_input, request_parameters, 
                            corr_function, request.numberOfResults)
            else:
                results, input_size = \
                    self._low_resolution_service.execute_search(
                        low_res_input, request_parameters, 
                        corr_function, request.number_of_results)
                        
                logging.info("Search finished in low resolution dataset, sending results of request id " + request.request_id)

            results_for_kafka: Dict = \
                self._kafka_protocol.convert_results_to_kafka_object(
                    results, input_size, 
                    request.number_of_results, 
                    corr_function.is_reverse_order())
            
            self._kafka_protocol.submit_result(request.request_id, results_for_kafka,self._node_id)

            if not self._low_resolution_service is None:
                logging.info("Waiting for kafka queue")

            if self._full_resolution_service.uses_global_candidates:
                results, input_size = \
                    self._process_candidates(
                        self._full_resolution_service, original_input_organized,
                        request, request_parameters, corr_function)
            else:
                results = \
                    self._full_resolution_service.execute_search_on_ts(
                        self._kafka_protocol.get_results_kafka(request), 
                        original_input_organized, request_parameters, 
                        corr_function,request.number_of_results)[0]
            
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
                #the created reduced files must also be deleted
                if not self._low_resolution_service is None and not low_res_input is None:
                    for file in low_res_input:
                        subprocess.call(["rm", file])

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