import logging
from typing import Dict, Iterable, List, Tuple
from controller.file_protocol.dtos.dataset_properties import InputFileProperties, factory_InputFileProperties
from service.service_main_structure import DatasetSelectionParameter, RequestParameters
from protocol.protocol_pb2 import DatasetInputPaths, DatasetSelectionParam, RequestExtraOptions   


def dataset_selection_parameters(ds_sel_param: DatasetSelectionParam) -> DatasetSelectionParameter:
    """Builds the DatasetSelectionParameter object from the grpc object

    Args:
        ds_sel_param (DatasetSelectionParam): object received from grpc

    Returns:
        DatasetSelectionParameter: equivalent DatasetSelectionParameter object
    """
    return DatasetSelectionParameter(ds_sel_param.name,ds_sel_param.min, ds_sel_param.max)

def request_parameters_factory(request_options: RequestExtraOptions) -> RequestParameters:
    """Builds the RequestParameters object from the grpc object
    The RequestParameters is the object used by the services, so there is 
    a logic separation between the grpc protocol and the rest of the code.

    Args:
        request_options (RequestExtraOptions): received object from grpc

    Returns:
        RequestParameters: equivalent RequestParameters object
    """
    res: RequestParameters = RequestParameters()
    if len(request_options.used_data_var) != 0:
        res.search_data_var = list(request_options.used_data_var)

    param_list: List[DatasetSelectionParameter] = []
    for param in request_options.dataset_selection_params:
        param_list.append(dataset_selection_parameters(param))

    if request_options.ts_neighbour_gap != 0:
        res.ts_neighbour_gap = request_options.ts_neighbour_gap

    res.dataset_selection_parameters = param_list
    
    if len(request_options.search_hours) != 0:
        for value in request_options.search_hours:
            if value < 0 or value > 23:
                raise ValueError("The hours in the \"search hours \" parameter should be between 0 and 23")
        res.search_hours = list(request_options.search_hours)
    
    if len(request_options.input_step_difference) != 0:
        for value in request_options.input_step_difference:
            if value < 0:
                raise ValueError("The number of hours between input timestamps, has to be a non negative number")
        res.input_step_difference = list(request_options.input_step_difference)

    return res

def list_of_files_factory(files: Iterable[DatasetInputPaths]) -> List[InputFileProperties]:
    """Function that maps the input file names received from gRPC request into
    an object that can be used for the file transfer process

    Args:
        files (Iterable[DatasetInputPaths]): object with the files to be transfered
        received from the gRPC

    Returns:
        Dict[str, List[InputFileProperties]]: the same objects converted
    """
    res: List[InputFileProperties] = []

    for file in files:
        res += list(map(lambda obj: factory_InputFileProperties(obj,file.data_variable),
                         file.input_files))
    return res

    
def separate_files_by_data_vars(files: List[Tuple[str,InputFileProperties]]) -> Dict[str, List[str]]:
    """Separates the list of files resulting from the files downloaded by the FileProtocol
    by data variable

    Args:
        files (List[Tuple[str,InputFileProperties]]): result object from FileProtocol transfer

    Returns:
        Dict[str, List[Tuple[str,InputFileProperties]]]: elements in files separated by data variable
    """
    res: Dict[str,List[str]] = {}
    for file in files:
        logging.debug(res)
        if file[1].data_variable in res:
            res[file[1].data_variable].append(file[0])
        else:
            res[file[1].data_variable] = [file[0]]

    return res