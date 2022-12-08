from typing import Any, Dict, Tuple, Optional
import numpy as np
import xarray

from correlation_functions.correlation_statistics import CorrelationStatistics
from repository.repository_layer import RepositoryMetadata

"""The system allows the usage of different correlation functions.
The "CorrelationFunction" is the base class from which all implementations.

Some Correlation functions may require collecting some statistics from the
available data. That is the reason why the "setup_stats" is available. This 
was created as some correlation functions may require some data processing 
before any search to allow the creation of the candidates.
"""

class CorrelationFunction:
    """Main structure of the correlation function
    """

    def __init__(self, name: str, properties_path: Optional[str] = None) -> None:
        """Basic constructor. Only goal is to register
        the name of the correlation function

        Args:
            name (str): name of the correlation function
            properties_path (Optional[str]): path for the properties file
        """
        self._corr_func_name: str = name
        self._properties_path: Optional[str] = properties_path

    @property
    def corr_func_name(self) -> str:
        return self._corr_func_name
    
    @property
    def properties_path(self) -> Optional[str]:
        return self._properties_path

    @property
    def max_value(self) -> float:
        raise NotImplementedError("Method must be implemented")
    
    @property
    def min_value(self) -> float:
        raise NotImplementedError("Method must be implemented")
    

    def calculate(self, dataarray1: xarray.DataArray, dataarray2: xarray.DataArray, repository_metadata: RepositoryMetadata, variable: str) -> float:
        """Calculates the similirarity value

        Args:
            dataarray1 (xarray.DataArray): array one
            dataarray2 (xarray.DataArray): array two
            repository_metadata (RepositoryMetadata): metadata of repository
            variable (str): used variable in the data variables

        Returns:
            float: similarity value
        """
        raise NotImplementedError("Method must be overriden")

    def is_reverse_order(self) -> bool:
        """If values should be ordered in a non growing order

        Returns:
            bool: true if values should be ordered in a non growing order
        """
        raise NotImplementedError("Method must be overriden")

    def compare(self, value1: float, value2: float) -> bool:
        """Comparator to be used for this similarity value 

        Args:
            value1 (float): value 1
            value2 (float): value 2

        Returns:
            bool: true if value2 has a better similarity than value1
        """
        raise NotImplementedError("Method must be overriden")

    def setup_stats(self, dataarray: xarray.DataArray, repository_metadata: RepositoryMetadata,
        variable: str) -> CorrelationStatistics:
        """Gathers required statistics from the given data array

        Args:
            dataarray (xarray.DataArray): dataarray to be analysed
            repository_metadata (RepositoryMetadata): metadata of the used repository
            variable (str): name of the data variable being analysed

        """
        raise NotImplementedError("Method must be overriden")

    def calculate_partial_value(self,input_array: xarray.DataArray, dataset_array: xarray.DataArray, 
        input_stats: CorrelationStatistics, dataset_stats: CorrelationStatistics,
        selection_params: Dict[str, Any], repository_metadata: RepositoryMetadata, 
        variable: str) -> Tuple[float,float]:
        """Calculates the partial similarity value for a given portion of arrays and 
        the previously calculated statistics

        Args:
            input_array (xarray.DataArray): part of the array provided as an input
            dataset_array (xarray.DataArray): part of the array from the dataset
            input_stats (CorrelationStatistics): previously calculated stats from input
            dataset_stats (CorrelationStatistics): previously calculated stats from the 
            section of the dataset
            selection_params (Dict[str, Any]): selection parameters for the dataset
            repository_metadata (RepositoryMetadata): metadata of repository
            variable (str): used variable in the data variables

        Returns:
            Tuple[float,float]: the range with the first value as the best possible 
            result and the second as the worst possible result
        """
        raise NotImplementedError("Method must be overriden")
