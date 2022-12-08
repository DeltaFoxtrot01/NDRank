from datetime import datetime
import logging
from typing import Any, Dict, Final, Iterable, Iterator, List, Optional, Set, Tuple, Union, cast
import xarray
import numpy as np
import numpy.typing as npt
import pandas as pd
from auxiliar.ts_logger import get_ts_debug_handler
from auxiliar.xarray_aux import open_dataset_with_file_name
from correlation_functions.main_structure import CorrelationFunction
from repository.auxiliary_structures.dataset_indexer import DateContainer
from repository.auxiliary_structures.time_gap_container import TimeGapContainer
from repository.repository_collection import RepositoryCollection
from service.constants import SIMPLE_SERVICE
from service.data_types import InputIterator, ResultContainer
from service.service_main_structure import HeuristicResult, RequestParameters, ServiceLayer
from auxiliar.component_injector import component_injector
from repository.repository_layer import RepositoryLayer, RepositoryMetadata

MAIN_TIME_DIMENSION: Final = 'time'
USED_TIME_DIMENSION: Final = 'step'
STEP: Final = "step"

@component_injector.inject_service(SIMPLE_SERVICE)
class BruteForceService(ServiceLayer):
    """Service used to execute a brute force search


    """
    def _open_input_as_dataset_with_multiple_vars(self, 
        files: Dict[str,List[str]], request_params: RequestParameters) -> Tuple[Dict[str, InputIterator],int]:
        res: Dict[str, InputIterator] = {}
        input_size: Optional[int] = None
        for var in files:
            res[var] = self._open_input_as_dataset(files[var],request_params,var)
            if input_size is None:
                input_size = res[var].size
            else:
                if input_size != res[var].size:
                    raise ValueError("One of the inputs has a different size")
        if input_size is None:
            raise ValueError("There must be at least one Input list")
        return (res, input_size)

    def _open_input_as_dataset(self, file_paths: List[str], request_params: RequestParameters,data_var:str) -> InputIterator:
        aux: List[xarray.Dataset] = []
        if request_params.search_data_var is None:
            raise ValueError("_open_input_as_dataset requires search_data_var to be defined")

        for path in file_paths:
            aux.append(open_dataset_with_file_name(path)[[data_var]])

        return InputIterator(aux, self._repositories.get_metadata_by_data_var(data_var), 
                    request_params.search_data_var, request_params.input_step_difference)

    def execute_search(self, file_paths: Dict[str,List[str]], request_parameters: RequestParameters, corr_function: CorrelationFunction, 
        num_results: Optional[int] = None) -> Tuple[Dict[str, ResultContainer],int]:
        """
        Executes a full brute force search in the local portion of the 
        existing dataset

        Args:
            file_paths (Dict[str,List[str]]): files with the given input

        Returns:
            Dict[str, ResultContainer]: found results together with the starting 
            timestamp in the dataset
        """
        logging.info("Opening input files")
        debug_ts_logger: logging.Logger = get_ts_debug_handler()

        if request_parameters.search_data_var is None:
            raise ValueError("Brute force service requires for the data variable to be defined")

        repo_subset: RepositoryCollection = \
            self._repositories.get_subsection_repositories(request_parameters.search_data_var)

        input_size: int
        input_iterator_collection: Dict[str,InputIterator] 
        input_iterator_collection, input_size = \
            self._open_input_as_dataset_with_multiple_vars(file_paths, request_parameters)
        res: Dict[str, ResultContainer] = {}

        #the first step is to iterate all existing repositories to find out which have
        # the desired data variables
        for repository in repo_subset.repositories:
            metadata: RepositoryMetadata = repository.get_metadata()
            step_variation: np.timedelta64 = np.timedelta64(int(repo_subset.step_variation),'ns')
            #the datasets have two time dimensions:
            # - one that has a single timestamp (usually the first date of the given file)
            # - another will all step values of the existing time dimension
            # in order to obtain on actual timestamp, it is necessary to sum the the first
            #timestamp together with the used step value
            # This is valid for the data that has been processed
            step_values: npt.NDArray[np.timedelta64]
            time_date: Union[np.datetime64, npt.NDArray[np.datetime64]] #only a Union since can return both kinds
                                                                        #with .values attribute (even tho only the
                                                                        #np.datetime64 is relevant for this context)
            # from this point, then a sequencial iteration of each file is done one by one
            for dataset_pair in repository.get_dataset():
                logging.info("Searching file " + dataset_pair[0])
                step_values = dataset_pair[1].coords[metadata.time_variation_dim].values
                time_date = dataset_pair[1].coords[metadata.time_initial_dim].values
                iterable: bool = True

                if not isinstance(step_values, Iterable):
                    step_values = cast(npt.NDArray[np.timedelta64],np.array([step_values]))
                    iterable = False

                for step in step_values:
                    #verifies if the current timestamp is part of the gaps of data
                    # or the timestamps that were provided by the request
                    debug_ts_logger.debug("START OF SINGLE STEP")
                    if repo_subset.is_gap(step + time_date, request_parameters.search_data_var,request_parameters.search_hours):
                        logging.debug("Skipping " + str(time_date + step))
                        continue
                    params: Dict[str, Any] = {}
                    params[metadata.time_variation_dim] = step
                    dataset_section: xarray.Dataset
                    data_array_section: xarray.DataArray
                    input_array: xarray.DataArray

                    debug_ts_logger.debug("START SELECT OF STEP")
                    if iterable:
                        dataset_section = dataset_pair[1].sel(params)
                    else:
                        dataset_section = dataset_pair[1]
                    debug_ts_logger.debug("END SELECT OF STEP")

                    for var in request_parameters.search_data_var:
                        key: np.datetime64 = time_date + step
                        if var in metadata.data_vars:
                            input_iterator: InputIterator = input_iterator_collection[var]

                            for input_tuple in input_iterator.iterate():
                                input_ts: xarray.Dataset = input_tuple[0]
                                input_interval: int = input_tuple[1]

                                key -= step_variation * input_interval
                                debug_ts_logger.debug("START SELECT AND ARRAY CONVERSION")
                                data_array_section = dataset_section[[var]].to_array()
                                debug_ts_logger.debug("END SELECT AND ARRAY CONVERSION")

                                input_array = input_ts[var]
                                debug_ts_logger.debug("START CORRELATION")
                                sim_val_raw: float = corr_function.calculate(data_array_section, input_array, metadata, var)
                                debug_ts_logger.debug("END CORRELATION")
                                sim_val_raw /= len(request_parameters.search_data_var)

                                str_key: str = str(key)

                                if not str_key in res:
                                    res[str_key] = ResultContainer(sim_val_raw)
                                else:
                                    res[str_key].add_value(sim_val_raw)
                                key -= step_variation
                                while repo_subset.is_gap(key, request_parameters.search_data_var,request_parameters.search_hours):
                                    key -= step_variation

                            debug_ts_logger.debug("END OF SINGLE STEP")
                dataset_pair[1].close()

        return res, input_size

    def _get_file_from_heuristic(self, date: datetime,repository: RepositoryLayer) -> Optional[xarray.Dataset]:
        """Returns the file specific to the heuristic value

        Args:
            date (date): received date value
            repository (RepositoryLayer): repository being used

        Returns:
            Optional[xarray.Dataset]: found file of None if it is not found
        """
        year: int = date.year
        month: int = date.month
        day: int = date.day
        hour: int = date.hour
        
        container: DateContainer = DateContainer(year, month, day, hour)
        res: Optional[Tuple[str, xarray.Dataset]] = repository.get_dataset_part(container)
        if res is None:
            return None
        else:
            return res[1]

    def execute_search_on_ts(self, result_iterator: Iterator[HeuristicResult], file_paths: Dict[str,List[str]], 
        request_parameters: RequestParameters, corr_function: CorrelationFunction,
        num_results:Optional[int] = None) -> Tuple[Dict[str, ResultContainer], int]:

        if request_parameters.ts_neighbour_gap is None:
            raise ValueError("Search on timestamps in brute force search requires for the ts_neighbour_gap to be defined")

        if request_parameters.search_data_var is None:
            raise ValueError("Brute force service requires for the data variable to be defined")
        
        repo_subset: RepositoryCollection = \
            self._repositories.get_subsection_repositories(request_parameters.search_data_var)

        res: Dict[str, ResultContainer] = {}

        logging.info("Opening input files")
        ts_neighbour_gap: int = request_parameters.ts_neighbour_gap
        logging.debug("Size of neighbourhood: " + str(ts_neighbour_gap))
        already_searched_ts: Dict[str,Set[np.datetime64]] = {}
        for var in request_parameters.search_data_var:
            already_searched_ts[var] = set()

        input_size: int
        input_iterator_collection: Dict[str,InputIterator] 
        input_iterator_collection, input_size = \
                self._open_input_as_dataset_with_multiple_vars(file_paths, request_parameters)

        #SECTION RESPONSIBLE FOR CALCULATING THE SIMILARITY VALUE
        for heuristic in result_iterator: #the result_iterator is iterated first, because it can only be iterated once
                                          # therefore, this has always to be on top of the loop
            received_date_heuristic: np.datetime64 = np.datetime64(heuristic.ts)
            heuristic_neighbour_list: List[np.datetime64] = []
            
            for var in request_parameters.search_data_var:
                repository: RepositoryLayer = repo_subset.get_repository_by_data_var(var)
                metadata: RepositoryMetadata = repository.get_metadata()
                step_variation: np.timedelta64 = np.timedelta64(int(repo_subset.step_variation),'ns')

                #calculate the neighbouring timestamps
                for i in range(-ts_neighbour_gap+1,ts_neighbour_gap):
                    heuristic_neighbour_list.append(received_date_heuristic + step_variation * i)
                
                logging.debug("Created neighbour list: " + str(heuristic_neighbour_list))
                
                for date_heuristic in heuristic_neighbour_list:
                    if date_heuristic in already_searched_ts[var] or \
                        repo_subset.is_gap(date_heuristic, request_parameters.search_data_var, request_parameters.search_hours):
                        continue
                    else:
                        already_searched_ts[var].add(date_heuristic)

                    dates64: List[np.datetime64] = []
                    input_iterator: InputIterator = input_iterator_collection[var]
                    input_da: Iterator[Tuple[xarray.Dataset, int]] = input_iterator.iterate()
                    logging.debug("Converted heuristic timestamp: " + str(date_heuristic))

                    #First step: gather all required datetimes for the given heuristic and the size of the input
                    date_aux: np.datetime64 = date_heuristic
                    for input_tuple in input_iterator.iterate():
                        date_aux += step_variation * input_tuple[1]
                        while repo_subset.is_gap(date_aux,request_parameters.search_data_var, request_parameters.search_hours):
                            date_aux += step_variation
                        dates64.append(date_aux)
                        date_aux += step_variation

                    #Second step: for every existing date verify if it exists in the local portion of the dataset
                    for date64 in dates64:
                        dataset_part: Optional[xarray.Dataset] = self._get_file_from_heuristic(pd.to_datetime(date64), repository)

                        input_ts: xarray.Dataset 
                        input_ts, _ = next(input_da)

                        #Verifies if the file exists
                        if dataset_part is None:
                            logging.info("File not found, continuing...")
                            continue
                                
                        #Third step: if the required data is available, calculate the respective step

                        #only a Union since can return both kinds
                        #with .values attribute (even tho only the
                        #np.datetime64 is relevant for this context)
                        time_date: np.datetime64 = dataset_part.coords[metadata.time_initial_dim].values # type: ignore

                        step: np.timedelta64 = date64 - time_date
                        params: Dict[str, Any] = {}
                        params[metadata.time_variation_dim] = step
                        dataset_section: xarray.Dataset = dataset_part.sel(params)

                        #Final step: calculate the similarity and store it in the result container
                        sim_val_raw: float = corr_function.calculate(dataset_section[var], input_ts[var], metadata, var)

                        sim_val_raw /= len(request_parameters.search_data_var)
                        str_key: str = str(date_heuristic)

                        if not str_key in res:
                            res[str_key] = ResultContainer(sim_val_raw)
                        else:
                            res[str_key].add_value(sim_val_raw)

                        dataset_part.close()

        return res, input_size