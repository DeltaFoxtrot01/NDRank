from datetime import datetime
import logging, os
import threading
import exceptiongroup
import pandas as pd
from schema import Schema, Or #type: ignore
from multiprocessing.pool import ThreadPool
from typing import Any, Dict, Final, Iterator, List, Optional, Tuple, Union

import grpc
from grpc import Channel

from node_client.file_protocol_client.file_protocol_client import FileTransferInstanceClient
from protocol.protocol_pb2 import DatasetInputPaths, DatasetProperties, DatasetSelectionParam, SearchRequest, SearchResponse
from protocol.protocol_pb2_grpc import ControllerServiceStub

"""This is the file is the interface to interact with the worker nodes
"""

#options tags
DATA_VARS: Final = "data-vars"
PARTIAL_DATASET_PARAMETERS: Final = "partial-dataset-parameters"
DATA_VAR_SELECTION: Final = "data-var-selection"
TS_NEIGHBOUR_GAP: Final = "ts-neighbour-gap"
SEARCH_HOURS: Final = "search-hours"
INPUT_STEP_DIFFERENCE: Final = "input-step-difference"
MIN: Final = "min"
MAX: Final = "max"
MAX_MESSAGE_LENGTH: Final = 10 * 1024 * 1024

class worker_node:
    """Basic dto with the required information to create
    a connection with a worker node"""
    def __init__(self, ip: str, port: int) -> None:
        self._ip: str = ip
        self._port: int = port

    @property
    def ip(self) -> str:
        return self._ip
    
    @property
    def port(self) -> int:
        return self._port

    def __repr__(self) -> str:
        return "IP: " + self._ip + "\tPORT: " + str(self._port)

class _channel_stub_pair:
    """Pair of a channel and a stub
    """
    
    def __init__(self, node: worker_node) -> None:
        """
        Basic constructor of the chanel stub pair.
        Stablishes connection imediately with worker node

        Args:
            ip (str): ip of the worker node
            port (int): port of the worker node
        """
        self._ip: str = node.ip
        self._port: int = node.port
        self._channel: Channel = grpc.insecure_channel(self._ip + ":" + str(self._port),
            options=[
                ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
                ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH),
            ],)

    @property
    def ip(self) -> str:
        return self._ip

    @property
    def port(self) -> int:
        return self._port

    @property
    def channel(self) -> Channel:
        return self._channel


class _worker_stub_pair(_channel_stub_pair):

    def __init__(self, node: worker_node) -> None:
        super().__init__(node)
        self._stub: ControllerServiceStub = ControllerServiceStub(self._channel)

    @property
    def stub(self) -> ControllerServiceStub:
        return self._stub


class SearchRequestOptionsDto:
    """Class that stores the "options" block from the requests file
    """
    _partial_ds_params_schema: Schema = Schema(
        {
            str:
                {
                    MIN: Or(float,int),
                    MAX: Or(float,int)
                }
        })

    """Additional parameters for search request
    """
    def __init__(self, options: Dict[str, Any]) -> None:
        self._search_data_var: Optional[List[str]] = None
        self._partial_dataset_parameters: \
            Optional[Dict[str,Dict[str,Dict[str,float]]]] = None
        self._data_vars_selection: Optional[List[str]] = None
        self._ts_neighbour_gap: Optional[int] = None
        self._search_hours: Optional[List[int]] = None
        self._input_step_difference: Optional[List[int]] = None

        if DATA_VARS in options:
            if not isinstance(options[DATA_VARS], list):
                raise TypeError("Data vars should come in the format of a list")
            self._search_data_var = options[DATA_VARS]
        
        if PARTIAL_DATASET_PARAMETERS in options:
            self._partial_dataset_parameters = options[PARTIAL_DATASET_PARAMETERS]
            SearchRequestOptionsDto\
                ._partial_ds_params_schema\
                .validate(self._partial_dataset_parameters)
        
        if TS_NEIGHBOUR_GAP in options:
            self._ts_neighbour_gap = options[TS_NEIGHBOUR_GAP]

        if SEARCH_HOURS in options:
            self._search_hours = options[SEARCH_HOURS]

        if INPUT_STEP_DIFFERENCE in options:
            self._input_step_difference = options[INPUT_STEP_DIFFERENCE]

        if DATA_VAR_SELECTION in options:
            self._data_vars_selection = options[DATA_VAR_SELECTION]

    @property
    def search_data_var(self) -> Optional[List[str]]:
        return self._search_data_var

    @search_data_var.setter
    def search_data_var(self, search_data_var: List[str]) -> None:
        self._search_data_var = search_data_var

    @property
    def partial_dataset_params(self) -> Optional[Dict[str,Any]]:
        return self._partial_dataset_parameters

    @partial_dataset_params.setter
    def partial_dataset_params(self, partial_dataset_params: Dict[str,Any]) -> None:
        self._partial_dataset_parameters = partial_dataset_params

    @property
    def ts_neighbour_gap(self) -> Optional[int]:
        return self._ts_neighbour_gap

    @ts_neighbour_gap.setter
    def ts_neighbour_gap(self, value: int) -> None:
        self._ts_neighbour_gap = value

    @property
    def search_hours(self) -> Optional[List[int]]:
        return self._search_hours

    @property
    def input_step_difference(self) -> Optional[List[int]]:
        return self._input_step_difference

    @property
    def data_vars_selection(self) -> Optional[List[str]]:
        return self._data_vars_selection

    def __str__(self) -> str:
        return "Search Data Variable: " + "None" \
            if self._search_data_var is None else "'" + str(self._search_data_var) + "'"

class SearchRequestDto:
    """Dto argument for the search process
    """

    def _organize_data_paths(self, input_file_paths: Dict[str,List[str]]) -> Dict[str, List[Tuple[str,int]]]:
        """Reads the file paths and organizes them in tuples with the file path and the size of the file

        Args:
            input_file_paths (Dict[str,List[str]]): receives file paths

        Returns:
            Dict[str, List[Tuple[str,int]]]: dictionary of lists organized by data variables and
            tuples with the respective file paths and file sizes
        """
        res: Dict[str, List[Tuple[str,int]]] = {}
        for data_var in input_file_paths:
            elem: List[Tuple[str,int]] = \
                list(map(lambda file_name: (file_name, os.path.getsize(file_name)), input_file_paths[data_var]))
            res[data_var] = elem
        return res
    
    def __init__(self, request_name:str, number_of_results: Optional[int], 
                input_file_paths: Dict[str,List[str]], time_instances: int, 
                start_date: datetime, end_date: datetime, 
                options: Dict[str, Any], correlation_function: str):
        """Basic constructor

        Args:
            request_name (str): Name assigned to the request
            number_of_results (int): Request on the number of results. Defaults to None.
            input_file_paths (Dict[str,List[str]]): Paths of the file inputs organized by data variables
            time_instances (int): Number of time instances of the input
            start_date (datetime): starting year of the full dataset 
            end_date (datetime): ending year of the full dataset
            options: (Dict[str,Any]): optional options of the request
            correlation_function (str): name of the correlation function
            input_hour_difference (Optional[List[int]]): number of hours each time 
            instance should have apart 
        """
        self._request_name: str = request_name
        self._number_of_results: Optional[int] = number_of_results
        self._time_instances: int = time_instances
        self._start_date: datetime = start_date
        self._end_date: datetime = end_date
        self._input_file_paths: Dict[str,List[Tuple[str,int]]] = self._organize_data_paths(input_file_paths)
        self._search_options: SearchRequestOptionsDto = SearchRequestOptionsDto(options)
        self._corr_func: str = correlation_function

    @property
    def request_name(self) -> str:
        return self._request_name

    @property
    def number_of_results(self) -> Optional[int]:
        return self._number_of_results

    @property
    def time_instances(self) -> int:
        return self._time_instances

    @property
    def input_file_paths(self) -> Dict[str,List[Tuple[str, int]]]:
        return self._input_file_paths

    @property
    def correlation_function(self) -> str:
        return self._corr_func

    @property
    def search_options(self) -> SearchRequestOptionsDto:
        return self._search_options

    def is_in_dataset_interval(self, given_date:str) -> bool:
        """Compares if given date is between the dataset dates

        Args:
            given_date (str): string in timestamp forma to test

        Returns:
            bool: True if it is between dataset dates
        """
        date: datetime = pd.to_datetime(given_date)
        return self._start_date <= date <= self._end_date


    def __repr__(self) -> str:
        return "Request Name: " + self._request_name + \
               "\tNum. of Results: " + str(self._number_of_results) +\
               "\tInput paths: " + str(self._input_file_paths)

    def __str__(self) -> str:
        return "Request Name: " + self._request_name + \
               "\nNum. of results: " + str(self._number_of_results) +\
               "\nInput Paths: " + str(self._input_file_paths) +\
               "\nOptions: " + str(self._search_options)

    def to_SearchRequest(self) -> SearchRequest:
        """Factory method that converts from the 
        SearchRequestDto to the SearchRequest object.

        Returns:
            SearchRequest: object equivalent in the SearchRequest type
        """
        #builds the arguments for the RPC object, in order to organize input
        #files by data variable
        input_files: List[DatasetInputPaths] = []
        for data_var in self._input_file_paths:
            file_paths: List[DatasetProperties]
            file_paths = \
            list(map(lambda file_size_pair: 
                            DatasetProperties(file_name=file_size_pair[0], 
                                              size_of_dataset=file_size_pair[1]),
                     self._input_file_paths[data_var]))
            elem: DatasetInputPaths = DatasetInputPaths(data_variable=data_var)
            elem.input_files.extend(file_paths)
            
            input_files.append(elem)

        res: SearchRequest = SearchRequest()
        res.number_of_results = self._number_of_results if not self._number_of_results is None else -1
        res.input_files.extend(input_files)
        res.request_id = self._request_name
        res.correlation_function = self._corr_func

        if not self._search_options.search_data_var is None:
            res.options.used_data_var.extend(self._search_options.search_data_var)
        
        if not self._search_options.ts_neighbour_gap is None:
            res.options.ts_neighbour_gap = self._search_options.ts_neighbour_gap

        if not self._search_options.partial_dataset_params is None:
            for param in self._search_options.partial_dataset_params.keys():
                ds_sel_param: DatasetSelectionParam = DatasetSelectionParam()
                ds_sel_param.min = \
                    self._search_options.partial_dataset_params[param][MIN]
                ds_sel_param.max = \
                    self._search_options.partial_dataset_params[param][MAX]
                ds_sel_param.name = param
                res.options.dataset_selection_params.append(ds_sel_param)
        
        if not self._search_options.search_hours is None:
            res.options.search_hours.extend(self._search_options.search_hours)
        
        if not self._search_options.input_step_difference is None:
            res.options.input_step_difference.extend(self._search_options.input_step_difference)

        if not self._search_options.data_vars_selection is None:
            res.options.selection_data_vars.extend(self._search_options.data_vars_selection)

        return res

class Analogue:
    """Represents the location in timestamp format of the found analogue
    """
    def __init__(self, timestamp: str, similarity: float, time_instances: int) -> None:
        """Basic constructor

        Args:
            timestamps (str): first timestamp representing the result
            similarity (float): similarity value of the analogue
            time_instances (int): number of instances already summed
        """
        self._timestamp: str = timestamp
        self._similarity: float = similarity
        self._time_instances: int = time_instances
    
    @property
    def timestamp(self) -> str:
        return self._timestamp
    
    @property
    def similarity(self) -> float:
        return self._similarity

    @property
    def time_instances(self) -> int:
        return self._time_instances

    def __str__(self) -> str:
        return "Similarity: " + str(self._similarity) + "\tTimestamp: " + str(self._timestamp) + "\tNum of time instances: " + str(self._time_instances)


class SearchResultDto:
    """Container dto of all resulting analogues
    """
    def __init__(self) -> None:
        """Basic constructor
        """
        self._analogues: List[Analogue] = []
        self._is_reverse_order: bool = False

    @property
    def analogues(self) -> List[Analogue]:
        return self._analogues

    def add_analogue(self, analogue: Analogue) -> None:
        """Adds an analogue to the container

        Args:
            analogue (analogue): analogue to be added
        """
        self._analogues.append(analogue)

    @property
    def is_reverse_order(self) -> bool:
        return self._is_reverse_order

    @is_reverse_order.setter
    def is_reverse_order(self, value: bool) -> None:
        self._is_reverse_order = value

    def __str__(self) -> str:
        res: str = ""

        for analogue_res in self._analogues:
            res += str(analogue_res) + "\n"

        return res

    def __repr__(self) -> str:
        return str(self)

    def reduce_results_to_specific_number_of_results(self, number_of_results: int) -> None:
        """Cuts the number of results to a specific number

        Args:
            number_of_results (int): number of wanted results
        """
        if number_of_results <= 0:
            raise ValueError("number_of_results must be a positive number")
        self._analogues = self._analogues[:number_of_results]
    
    def list_struct_to_csv(self) -> Tuple[List[str], List[List[Any]]]:
        """Returns the results in a list format that allows the 
        storage in a CSV format

        Returns:
            Tuple[List[str], List[List[Any]]]: Two lists. The first is the headers, the second is the values
        """
        headers: List[str] = ["Timestamp", "Similarity"]
        values: List[List[Any]] = []

        for analogue in self._analogues:
            values.append([str(analogue.timestamp), analogue.similarity])
        
        return headers, values

class ClientModule:
    """Class to establish contact will all existing worker nodes
    """

    def __init__(self, worker_nodes: List[worker_node]):
        """Basic constructor for the client_module. Stablishes
        connection with all existing nodes.

        Args:
            worker_nodes (List[worker_node]): all nodes available or all 
        """
        self._worker_connections: List[_worker_stub_pair] = []
        self._errors: List[Exception] = []
        self._lock: threading.Lock = threading.Lock()

        for node in worker_nodes:
            self._worker_connections.append(_worker_stub_pair(node))
        
    def _search_request_single_node(self, request: SearchRequestDto, channel_pair: _worker_stub_pair) -> SearchResultDto:
        """Executes the search for a single worker node

        Args:
            request (SearchRequestDto): input provided to the search request
            channel_pair (_worker_stub_pair): node objects required to execute request

        Returns:
            SearchResultDto: results returned by the worker node
        """
        res: SearchResultDto = SearchResultDto()
        try:
            search_request: SearchRequest = request.to_SearchRequest()
            search_iterator: Iterator[SearchResponse] = channel_pair.stub.search_analogues(search_request)

            logging.info("Mapping ports for node " + str(channel_pair.ip))
            port_mapping: SearchResponse = next(search_iterator)
        
            logging.debug(port_mapping)

            file_transfer_instances: List[FileTransferInstanceClient] = []

            logging.info("Tranfering files for node " + str(channel_pair.ip))
            for port_file_pair in port_mapping.mappings:
                file_tranfer: FileTransferInstanceClient = \
                    FileTransferInstanceClient(port_file_pair, channel_pair.ip)
                file_transfer_instances.append(file_tranfer)    
        
            thread_pool: ThreadPool = ThreadPool(len(file_transfer_instances))

            thread_pool.map(lambda f_t: f_t.upload(), file_transfer_instances, chunksize=1)
            logging.info("Transfer finished for node " + str(channel_pair.ip))

            thread_pool.close()
            thread_pool.join()

            for f_t in file_transfer_instances:
                if not f_t.exception is None:
                    self._lock.acquire()
                    self._errors.append(f_t.exception)
                    self._lock.release()
                    return res

            logging.info("Waiting for returned results from " + str(channel_pair.ip))
            search_response: SearchResponse = next(search_iterator)
            for received_analogue in search_response.analogues:
                res.add_analogue(Analogue(received_analogue.timestamp, received_analogue.similarity_value,received_analogue.time_instances))

            res.is_reverse_order = search_response.reverse_sort_order_corr_function
            logging.info("Results received from " + str(channel_pair.ip))
        
            return res
        except grpc.RpcError as e:
            logging.debug("Exception received: ")
            self._lock.acquire()
            self._errors.append(e)
            self._lock.release()
            return res

    def _wait_for_low_res_conclusion(self, iterators: List[Optional[Iterator[SearchResponse]]]) -> None:
        """Waits for the low resolution nodes to finish in order to catch received errors

        Args:
            iterators (List[Optional[Iterator[SearchResponse]]]): received iterators

        Raises:
            Exception: If an error was received, it will raise it again as a mere "Exception"
        """
        for iterator in iterators:
            if not iterator is None:
                try:
                    next(iterator)
                except StopIteration:
                    continue
                except Exception as e:
                    raise Exception("Exception received from a low resolution node: " + str(e))

    def _merge_results(self,request: SearchRequestDto, results: List[SearchResultDto]) -> SearchResultDto:
        """Merges all results returned by all nodes into a single result

        Args:
            request (SearchRequestDto): received request
            results (List[SearchResultDto]): received results

        Returns:
            SearchResultDto: a single result
        """
        res: SearchResultDto = SearchResultDto()
        aux: Dict[str, List[Union[float,int]]] = {}

        for result in results:
            for analogue in result.analogues:

                if request.is_in_dataset_interval(analogue.timestamp):
                    if not analogue.timestamp in aux:
                        aux[analogue.timestamp] = [analogue.similarity, analogue.time_instances]
                    else:
                        aux[analogue.timestamp] = \
                            [
                                aux[analogue.timestamp][0] + analogue.similarity, 
                                aux[analogue.timestamp][1] + analogue.time_instances
                            ]

        if request.search_options.search_data_var is None:
            raise ValueError("Search data vars should be defined")

        for item in aux:
            #to verify if the results received are complete, it must verify if time instances and 
            #data variables were available, otherwise that value must be discarded
            if aux[item][1] == request.time_instances * len(request.search_options.search_data_var):
                #it is not using the number of data variables as well because it is assumed that 
                # the worker nodes already make that division
                res.add_analogue(
                    Analogue(item, aux[item][0]/(request.time_instances), request.time_instances))
            else:
                logging.info("Timestamp " + str(item) + " did not have all time instances")

        is_reverse: bool 
        if len(results) == 0:
            is_reverse = True
        else:
            is_reverse = results[0].is_reverse_order

        res.analogues.sort(key=lambda analogue: analogue.similarity, reverse=is_reverse)
        
        if not request.number_of_results is None:
            res.reduce_results_to_specific_number_of_results(request.number_of_results)

        return res

    def search_request(self, request: SearchRequestDto) -> SearchResultDto:
        """Creates a search request for all worker nodes

        Args:
            request (SearchRequestDto): search arguments

        Returns:
            SearchResultDto: analogues in timestamps ordered by similarity
        """
        res: List[SearchResultDto]
       
        thread_pool: ThreadPool = ThreadPool(len(self._worker_connections))

        logging.info("Executing search request on all nodes")
        res = thread_pool.map(lambda conn: self._search_request_single_node(request, conn), 
                              self._worker_connections)

        thread_pool.close()
        thread_pool.join()
        
        if len(self._errors) != 0:
            raise exceptiongroup.ExceptionGroup(
                "Exceptions thrown from sockets dealing with file transfer", 
                self._errors)

        logging.info("Merging final results")
        return self._merge_results(request,res)
