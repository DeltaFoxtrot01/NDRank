from datetime import datetime
import logging
import math
from typing import Any, Dict, Final, Optional, Tuple
import pandas
import xarray
import numpy as np
import yaml
from auxiliar.aux_methods import fix_path

from auxiliar.component_injector import component_injector
from auxiliar.xarray_aux import open_dataarray_with_file_name
from correlation_functions.correlation_statistics import CorrelationStatistics
from correlation_functions.main_structure import CorrelationFunction
from repository.repository_layer import RepositoryMetadata

CORRELATION_FUNCTIONS: Final = "correlation-functions"
AVERAGE_FOLDER: Final = "average-path"
STANDARD_DEVIATION_FOLDER: Final = "standard-deviation-path"
AVERAGE_FILE_NAME: Final = "average.yaml"
STANDARD_DEVIATION_FILE_NAME: Final = "standard_deviation.yaml"

def process_properties(properties_path: str) -> Dict[str, Any]:
    """Processes the properties file to retrieve required properties
    for correlation function

    Args:
        properties_path (str): path for the properties file

    Raises:
        ValueError: if the block "correlation-functions" does not exist

    Returns:
        Dict[str, Any]: properties object
    """
    properties: Dict[str,Any]
    with open(properties_path, 'r') as f:
        properties = yaml.safe_load(f)
    if not CORRELATION_FUNCTIONS in properties:
        raise ValueError("Properties must have correlation-functions block")
    return properties

class ParameterFileCollection:
    """Manages the parameter properties files
    and facilitates indexing
    """

    def __init__(self, path: str, file_name: str) -> None:
        """Basic contructor. Opens the file and stores the required
        structures to index these files

        Args:
            path (str): path where all the files can be found
            file_name (str): name of the yaml file that indexes existing files
        """
        self._files: Dict[int,Dict[int,Dict[int,str]]] #the keys go in the following order: month,day,hour
        self._file_path: str = path
        with open(self._file_path + file_name, 'r') as f:
            self._files = yaml.safe_load(f)

    def get_array(self, month: int, day: int, hour: int) -> xarray.DataArray:
        """Indexes file that matches the given day

        Args:
            month (int): month required
            day (int): day required
            hour (int): hour required

        Returns:
            str: path of the file
        """
        return open_dataarray_with_file_name(self._file_path + self._files[month][day][hour])


@component_injector.inject_correlation_function("pcc")
class Pcc(CorrelationFunction):
    """Implementation of the Pearson Correlation Coefficient
    """
    def calculate(self, dataarray1: xarray.DataArray, dataarray2: xarray.DataArray, repository_metadata: RepositoryMetadata, variable: str) -> float:
        return xarray.corr(dataarray1,dataarray2).data.item()

    def is_reverse_order(self) -> bool:
        return True

    def compare(self, value1: float, value2: float) -> bool:
        return value1 < value2

    def setup_stats(self, dataarray: xarray.DataArray, repository_metadata: RepositoryMetadata, variable: str) -> CorrelationStatistics:
        return CorrelationStatistics({})

    @property
    def max_value(self) -> float:
        return 1

    @property
    def min_value(self) -> float:
        return -1
        

@component_injector.inject_correlation_function("enhanced_pcc")
class EnhancedPcc(CorrelationFunction):

    _MEAN: Final = "mean"
    _COUNT: Final = "count"

    @property
    def max_value(self) -> float:
        return 1

    @property
    def min_value(self) -> float:
        return -1
        
    """Implementation of the Pearson Correlation Coefficient
    """
    def __init__(self, name: str, properties_path: Optional[str] = None) -> None:
        super().__init__(name, properties_path)
        self._average_files: ParameterFileCollection
        self._standard_deviation_files: ParameterFileCollection

        if not properties_path is None:
            self._setup_properties()

    def _setup_properties(self) -> None:
        properties: Dict[str, Any]
        if self._properties_path is None:
            raise ValueError("EnhancedPcc requires access to the properties path")
        properties = process_properties(self._properties_path)
        average_path: str = fix_path(properties[CORRELATION_FUNCTIONS][AVERAGE_FOLDER])
        std_deviation_path: str = fix_path(properties[CORRELATION_FUNCTIONS][STANDARD_DEVIATION_FOLDER])
        self._average_files = ParameterFileCollection(average_path, AVERAGE_FILE_NAME)
        self._standard_deviation_files = ParameterFileCollection(std_deviation_path, STANDARD_DEVIATION_FILE_NAME)

    def _process_arrays_correctly(self,dataarray1: xarray.DataArray, 
        repository_metadata: RepositoryMetadata, variable: str, 
        selection_params: Optional[Dict[str, Any]] = None) -> xarray.DataArray:
        """Treats the array with the given parameter

        Args:
            dataarray1 (xarray.DataArray): given array 1
            repository_metadata (RepositoryMetadata): origin repository metadata
            variable (str): used variable
            selection_params (Optional[Dict[str,Any]]): parameters used to select on 
            the array. Defaults to None

        Returns:
            Tuple[xarray.DataArray, xarray.DataArray]: arrays processed
        """
        if self._properties_path is None:
            raise ValueError("EnhancedPcc requires access to the properties path")
        time_var: str = repository_metadata.time_variation_dim
        time_init: str = repository_metadata.time_initial_dim

        dt1: np.datetime64 = dataarray1[time_var].values + dataarray1[time_init].values

        if not isinstance(dt1, np.datetime64):
            dt1 = dt1[0]

        ts1: datetime = pandas.to_datetime(dt1)

        average1: xarray.DataArray

        std_dev_1: xarray.DataArray

        if selection_params is None:
            average1 = self._average_files.get_array(ts1.month, ts1.day, ts1.hour).sel(variable=variable)
            std_dev_1 = self._standard_deviation_files.get_array(ts1.month, ts1.day, ts1.hour).sel(variable=variable)
        else:
            average1 = self._average_files.get_array(ts1.month, ts1.day, ts1.hour).sel(selection_params).sel(variable=variable)
            std_dev_1 = self._standard_deviation_files.get_array(ts1.month, ts1.day, ts1.hour).sel(selection_params).sel(variable=variable)

        #this operation is necessary to avoid division by zero
        std_dev_1 = std_dev_1.where(std_dev_1.values != 0).fillna(10**(-10))
        return (dataarray1 - average1)/std_dev_1

    def calculate(self, dataarray1: xarray.DataArray, dataarray2: xarray.DataArray, repository_metadata: RepositoryMetadata, variable: str) -> float:
        dataarray1 = self._process_arrays_correctly(dataarray1,repository_metadata,variable)
        dataarray2 = self._process_arrays_correctly(dataarray2,repository_metadata,variable)

        return xarray.corr(dataarray1,dataarray2).data.item()

    def calculate_partial_value(self, input_array: xarray.DataArray, dataset_array: xarray.DataArray, 
        input_stats: CorrelationStatistics, dataset_stats: CorrelationStatistics, 
        selection_params: Dict[str,Any], repo_metadata: RepositoryMetadata, var: str) -> Tuple[float, float]:
        #TODO: put image explaining what is going on here
        input_array = self._process_arrays_correctly(input_array,repo_metadata,var,selection_params)
        dataset_array = self._process_arrays_correctly(dataset_array,repo_metadata,var,selection_params)
        input_values: Dict[str, Any] = input_stats.get_values()
        array_values: Dict[str, Any] = dataset_stats.get_values()

        #first step: calculate components with partial values
        #top component
        top_sum: float = \
            ((input_array - input_values[EnhancedPcc._MEAN]) * 
            (dataset_array - array_values[EnhancedPcc._MEAN])).sum().values.item()

        #bottom component, input array
        bottom_sum_input: float = \
            ((input_array - input_values[EnhancedPcc._MEAN])**2).sum().values.item()     

        #bottom component, dataset array
        bottom_sum_dataset: float = \
            ((dataset_array - array_values[EnhancedPcc._MEAN])**2).sum().values.item()


        #second step: calculate partial values
        full_count: int = array_values[EnhancedPcc._COUNT]
        partial_count: int = dataset_array.count().values.item()
        missing_count: int = full_count - partial_count

        #top component
        top_sum_missing_best: float = \
            (((1 - input_values[EnhancedPcc._MEAN]) * 
            (1 - array_values[EnhancedPcc._MEAN])) * missing_count)
        top_sum_missing_worst: float = \
            (((-1 - input_values[EnhancedPcc._MEAN]) * 
            (-1 - array_values[EnhancedPcc._MEAN])) * missing_count)


        #bottom component, input array        
        bottom_sum_input_best: float = \
            ((1 - input_values[EnhancedPcc._MEAN])**2) * missing_count  
        bottom_sum_input_worst: float = \
            ((-1 - input_values[EnhancedPcc._MEAN])**2) * missing_count


        #bottom component, dataset array
        bottom_sum_dataset_best: float = \
            ((1 - array_values[EnhancedPcc._MEAN])**2) * missing_count
        bottom_sum_dataset_worst: float = \
            ((-1 - array_values[EnhancedPcc._MEAN])**2) * missing_count


        #final step: calculate the best and worst values
        best_value: float = \
            (top_sum + top_sum_missing_best) /\
            (
                ((bottom_sum_input + bottom_sum_input_best)*
                (bottom_sum_dataset + bottom_sum_dataset_best))**(1/2)
            )
        worst_value: float = \
            (top_sum + top_sum_missing_worst) /\
            (
                ((bottom_sum_input + bottom_sum_input_worst)*
                (bottom_sum_dataset + bottom_sum_dataset_worst))**(1/2)
            )
        
        if best_value < worst_value:
            best_value, worst_value = worst_value, best_value

        return (best_value,worst_value)

    def is_reverse_order(self) -> bool:
        return True

    def compare(self, value1: float, value2: float) -> bool:
        return value1 < value2

    def setup_stats(self, dataarray: xarray.DataArray,repository_metadata: 
        RepositoryMetadata, variable: str) -> CorrelationStatistics:
        dataarray = self._process_arrays_correctly(dataarray, repository_metadata, variable)
        mean_val: float = dataarray.mean().values.item()
        count: int = dataarray.count().values.item()

        dict: Dict[str,Any] = {}
        dict[EnhancedPcc._MEAN] = mean_val
        dict[EnhancedPcc._COUNT] = count
        
        return CorrelationStatistics(dict)


@component_injector.inject_correlation_function("rmsd")
class Rmsd(CorrelationFunction):
    """Implementation of the Root Mean Square Distance"""

    _MAX: Final = "max"
    _MIN: Final = "min"
    _COUNT: Final = "count"

    def calculate(self, dataarray1: xarray.DataArray, dataarray2: xarray.DataArray, 
        repository_metadata: RepositoryMetadata, variable: str) -> float:
        aux_datarray: xarray.DataArray = dataarray1 - dataarray2
        aux_datarray = aux_datarray**2
        sum_value: float = aux_datarray.sum().values.item()
        div_value: float = sum_value / dataarray1.count().values.item()
        return math.sqrt(div_value)

    def is_reverse_order(self) -> bool:
        return False

    def compare(self, value1: float, value2: float) -> bool:
        return value1 > value2

    def setup_stats(self, dataarray: xarray.DataArray, repository_metadata: RepositoryMetadata, 
        variable: str) -> CorrelationStatistics:
        max_val: float = dataarray.max().values.item()
        min_val: float = dataarray.min().values.item()
        count: int = dataarray.count().values.item()

        dict: Dict[str,Any] = {}
        dict[Rmsd._MAX] = max_val
        dict[Rmsd._MIN] = min_val
        dict[Rmsd._COUNT] = count
        
        return CorrelationStatistics(dict)

    def calculate_partial_value(self, input_array: xarray.DataArray, dataset_array: xarray.DataArray, 
        input_stats: CorrelationStatistics, dataset_stats: CorrelationStatistics, 
        selection_params: Dict[str,Any],repo_metadata: RepositoryMetadata, var: str) -> Tuple[float, float]:
        common_value: float
        min_value: float
        max_value: float
        stats_dict: Dict[str,Any] = dataset_stats.get_values()
        input_stats_dict: Dict[str,Any] = input_stats.get_values()
        total_count: int = stats_dict[Rmsd._COUNT]

        aux_dataarray: xarray.DataArray = input_array-dataset_array
        aux_dataarray = aux_dataarray**2
        common_value = aux_dataarray.sum().values.item()
        common_value = common_value/total_count

        min_value = math.sqrt(common_value)

        max_combination: float = abs(input_stats_dict[Rmsd._MAX] - stats_dict[Rmsd._MAX])
        max_combination = max(max_combination, abs(input_stats_dict[Rmsd._MAX] - stats_dict[Rmsd._MIN]))
        max_combination = max(max_combination, abs(input_stats_dict[Rmsd._MIN] - stats_dict[Rmsd._MAX]))
        max_combination = max(max_combination, abs(input_stats_dict[Rmsd._MIN] - stats_dict[Rmsd._MIN]))
        partial_count: int = input_array.count().values.item()

        max_value = math.sqrt(common_value + ((max_combination**2) *(total_count - partial_count))/total_count)

        return (min_value, max_value)