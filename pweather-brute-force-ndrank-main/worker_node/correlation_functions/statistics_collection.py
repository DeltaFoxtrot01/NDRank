import logging
from typing import Dict, List, Tuple

import numpy as np
import xarray
from auxiliar.component_injector import component_injector
from correlation_functions.main_structure import CorrelationFunction
from correlation_functions.correlation_statistics import CorrelationStatistics
from repository.repository_layer import RepositoryMetadata


class StatisticsCollection:
    """Class responsible for storing the collected statistics from the correlation
    functions and provide access to those statistics.
    """
    def __init__(self) -> None:
        """Simple constructor

        The container is internally represented by a group of dictionaries indexed 
        first by the name of the correlation function, then by the data variable and 
        only then by the timestamp it reffers to
        
        (correlation_function name, data_var, timestamp)
        """
        self._container: Dict[str, Dict[str,Dict[np.datetime64,CorrelationStatistics]]] = {}


    def insert_statistic(self, datetime: np.datetime64, dataset:xarray.Dataset, repository_metadata: RepositoryMetadata, skip_functions: List[str]) -> None:
        """Calculates the required statistics for all correlation functions

        Args:
            datetime (np.datetime64): corresponding datetime
            dataarray (xarray.Dataset): dataset section to be analysed
        """
        correlation_functions: List[Tuple[str, CorrelationFunction]] = \
            component_injector.get_all_correlation_function_instances()
        
        for function in correlation_functions:
            if function[0] in skip_functions:
                continue
            
            if not function[0] in self._container:
                self._container[function[0]] = {}

            corr_function_container: Dict[str,Dict[np.datetime64, CorrelationStatistics]] = \
                self._container[function[0]]
            for var in dataset.data_vars:
                if not var in corr_function_container:
                    corr_function_container[str(var)] = {}
                
                datarray: xarray.DataArray = dataset[var]
                corr_function_container[str(var)][datetime] = \
                    function[1].setup_stats(datarray,repository_metadata,str(var))
        
    def get_statistics(self, correlation_function_name:str, var_name:str, datetime:np.datetime64) -> CorrelationStatistics:
        """Returns the required statistics to calculate the similarity interval

        Args:
            correlation_function_name (str): name of the correlation function
            var_name (str): name of the variable to index by
            datetime (np.datetime64): date corresponding to the given statistics

        Raises:
            ValueError: if the correlation function or the datetime can not be found

        Returns:
            StatisticsHolder: corresponding statistics holder
        """
        if not correlation_function_name in self._container:
            raise ValueError("Correlation Function " + correlation_function_name + \
                " does not exist")
        if not var_name in self._container[correlation_function_name]:
            raise ValueError("Data variable " + var_name + " does not exist for correlation metric " + \
                correlation_function_name)
        if not datetime in self._container[correlation_function_name][var_name]:
            raise ValueError("Timestamp " + str(datetime) + " does not exist for correlation metric " + \
                correlation_function_name + " for the data variable " + var_name)
        return self._container[correlation_function_name][var_name][datetime]

    def print_statistics(self) -> None:
        """Prints collected values"""
        for corr_func_name in self._container:
            for data_var in self._container[corr_func_name]:
                for timestamp in self._container[corr_func_name][data_var]:
                    logging.debug(str(corr_func_name) + "-" + str(data_var) + "-" + str(timestamp) + ":\t" \
                        + str(self._container[corr_func_name][data_var][timestamp]))