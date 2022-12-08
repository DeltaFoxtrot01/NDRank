from typing import Dict, Iterator, List, Optional, Tuple, Type

import numpy as np
from controller.file_protocol.dtos.dataset_properties import InputFileProperties
from correlation_functions.main_structure import CorrelationFunction
from repository.repository_collection import RepositoryCollection
from repository.repository_layer import RepositoryLayer, RepositoryMetadata
from service.data_types import CandidateContainer, ResultContainer

class HeuristicResult:
    """Single result returned by the low resolution nodes
    """

    def __init__(self, ts: str, value_or_best_value: float, worst_value: Optional[float] = None) -> None:
        self._ts: str = ts
        self._value: Optional[float]
        self._best_value: Optional[float]
        self._worst_value: Optional[float]

        if not worst_value is None:
            self._best_value = value_or_best_value
            self._worst_value = value_or_best_value
            self._value = None
        else:
            self._best_value = None
            self._worst_value = None
            self._value = value_or_best_value

    @property
    def ts(self) -> str:
        """Timestamp where to search

        Returns:
            str: timestamp as string
        """
        return self._ts

    @property
    def value(self) -> float:
        """Calculated similarity value

        Raises:
            ValueError: if value is not defined

        Returns:
            float: similarity value as float
        """
        if self._value is None:
            raise ValueError("Value is not defined")
        return self._value

    @property
    def best_value(self) -> float:
        """Calculated best similarity value

        Raises:
            ValueError: if best_value is not defined

        Returns:
            float: best value as float
        """
        if self._best_value is None:
            raise ValueError("best_value is not defined")
        return self._best_value

    @property
    def worst_value(self) -> float:
        """Calculated worst similarity value

        Raises:
            ValueError: if worst_value is not defined

        Returns:
            float: worst value as float
        """
        if self._worst_value is None:
            raise ValueError("worst_value is not defined")
        return self._worst_value


class DatasetSelectionParameter:
    """Parameters used to obtain just a portion of the dataset
    """

    def __init__(self, name: str, min: float, max: float) -> None:
        self._name: str = name
        self._min: float = min
        self._max: float = max
        if min > max:
            raise ValueError("Parameter with name " + name + \
                " has a minimum value bigger than the maximum value. MAX: "\
                 + str(max) + ", MIN: " + str(min))

    @property
    def max(self) -> float:
        return self._max

    @property
    def min(self) -> float:
        return self._min

    @property
    def name(self) -> str:
        return self._name

class RequestParameters:
    """Container with all aditional parameters of the request"""
    def __init__(self) -> None:
        self._search_data_var: Optional[List[str]] = None
        self._ds_sel_param: Optional[List[DatasetSelectionParameter]] = None
        self._ts_neighbour_gap: Optional[int] = None
        self._search_hours: Optional[List[int]] = None
        self._input_step_difference: Optional[List[int]] = None
        self._selection_data_vars: Optional[List[str]] = None

    @property
    def search_data_var(self) -> Optional[List[str]]:
        """If it is necessary to search for one specific data variable 
        only. In case of null, all data variables will be used

        Returns:
            Optional[List[str]]: name of the data variable
        """
        return self._search_data_var
    
    @search_data_var.setter
    def search_data_var(self, value: List[str]) -> None:
        """To set the data variable that should be searched

        Args:
            value (List[str]): name of the data variable
        """
        self._search_data_var = value

    @property
    def dataset_selection_parameters(self) -> Optional[List[DatasetSelectionParameter]]:
        """Parameters used to selection a portion of each array. Intended to be used 
        in the list of candidates

        Returns:
            Optional[List[DatasetSelectionParameter]]: List of parameters
        """
        return self._ds_sel_param

    @dataset_selection_parameters.setter
    def dataset_selection_parameters(self, value: List[DatasetSelectionParameter]) -> None:
        self._ds_sel_param = value

    @property
    def ts_neighbour_gap(self) -> Optional[int]:
        """Parameter used to verify how what are the neighbour timestamps that should
        also be searched.

        Returns:
            Optional[int]: Size of the neighbouring gap
        """
        return self._ts_neighbour_gap
    
    @ts_neighbour_gap.setter
    def ts_neighbour_gap(self, value: int) -> None:
        if value <= 0:
            raise ValueError("ts_neighbour_gap has to be a positive number")

        self._ts_neighbour_gap = value

    @property
    def search_hours(self) -> Optional[List[int]]:
        """Hours that should be exclusively used to search in the dataset.
        If not defined, all hours should be searched

        Returns:
            Optional[List[int]]: List of hours that should be selected or None 
        if all should be searched
        """
        return self._search_hours

    @search_hours.setter
    def search_hours(self, value: List[int]) -> None:
        self._search_hours = value

    @property
    def input_step_difference(self) -> Optional[List[int]]:
        """Number of hours that should exist appart of each input instance"""
        return self._input_step_difference

    @input_step_difference.setter
    def input_step_difference(self, value: List[int]) -> None:
        self._input_step_difference = value

    @property
    def selection_data_vars(self) -> Optional[List[str]]:
        """Returns the data variables that should be used in global_parameter"""
        return self._selection_data_vars

    @selection_data_vars.setter
    def selection_data_vars(self, data_vars: List[str]) -> None:
        self._selection_data_vars = data_vars
    
    
class ServiceLayer:
    """
    Class for the representation of the service layer.

    In the context of this system, the service layer is 
    responsible for calculating the similarity value for
    the existing atmospheric states.

    This class defines the main structure any implementation
    of the service layer should have
    """

    def __init__(self, repositories: List[RepositoryLayer]) -> None:
        self._repositories: RepositoryCollection = RepositoryCollection(repositories)

    @property
    def repositories(self) -> RepositoryCollection:
        return self._repositories

    def execute_search(self, file_paths: Dict[str, List[str]], request_parameters: RequestParameters, 
        corr_function: CorrelationFunction, num_results:Optional[int] = None) -> Tuple[Dict[str, ResultContainer],int]:
        """Method to execute a full brute force search

        Args:
            file_paths (Dict[str, List[str]]): paths for the input files
            request_parameters (RequestParameters): extra parameters that can be used for the search process
            corr_function (CorrelationFunction): correlation function to be used
            num_results (Optional[int]): number of wanted results

        Returns:
            Tuple[Dict[str, ResultContainer],int]: Obtained results and size of the input in the time dimension
        """
        raise NotImplementedError("Method must be overriden")

    def execute_search_for_candidates(self, file_paths: Dict[str,List[str]], request_parameters: RequestParameters, 
        corr_function: CorrelationFunction, num_results:Optional[int] = None) -> Tuple[Dict[np.datetime64, CandidateContainer],int]:
        """Method to execute a full brute force search for candidates

        Args:
            file_paths (Dict[str,List[str]]): paths for the input files
            request_parameters (RequestParameters): extra parameters that can be used for the search process
            corr_function (CorrelationFunction): correlation function to be used
            num_results (Optional[int]): number of wanted results

        Returns:
            Tuple[Dict[str, CandidateContainer],int]: Obtained candidates and size of the input in the time dimension
        """
        raise NotImplementedError("Method must be overriden")

    def get_low_resolution_parameters(self, data_var: str) -> Dict[str, int]:
        """Requests the repository to return the required information
        to know how the resolution was reduced

        Args:
            data_var (str): specific data variable to find the needed repository

        Returns:
            Dict[str, int]: dimensions together with their resolution factor
        """
        return self._repositories.get_low_resolution_params_by_data_var(data_var)
    
    def execute_search_on_ts(self, result_iterator: Iterator[HeuristicResult], file_paths: Dict[str,List[str]], 
        request_parameters: RequestParameters, corr_function: CorrelationFunction,
        num_results:Optional[int] = None) -> Tuple[Dict[str, ResultContainer], int]:
        """Method to execute a search on the given timestamp

        Args:
            result_iterator (Iterator[HeuristicResult]): received timestamps
            file_paths (List[str]): paths for the input files
            request_parameters (RequestParameters): extra parameters that can be used for the search process
            num_results (Optional[int]): number of wanted results
        
        Returns:
            Tuple[Dict[str, ResultContainer], int]: received results
        """
        raise NotImplementedError("Method must be overriden")
    
    def execute_search_on_ts_for_candidates(self, result_iterator: Iterator[HeuristicResult], file_paths: List[str], 
        request_parameters: RequestParameters, corr_function: CorrelationFunction,
        num_results:Optional[int] = None) -> Tuple[Dict[str, CandidateContainer], int]:
        """Method to execute a search for candidates on the given timestamp 

        Args:
            result_iterator (Iterator[HeuristicResult]): received timestamps
            file_paths (List[str]): paths for the input files
            request_parameters (RequestParameters): extra parameters that can be used for the search process
            num_results (Optional[int]): number of wanted results
        
        Returns:
            Tuple[Dict[str, ResultContainer], int]: received results
        """
        raise NotImplementedError("Method must be overriden")
    
    @property
    def uses_global_candidates(self) -> bool:
        """Property used to check if controllers should first use a global list of candidates

        Returns:
            bool: True if it uses a list of candidates 
        """
        return False
    