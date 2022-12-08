import logging
from typing import Any, Dict, Iterator, List, Optional, Tuple
import numpy as np
import numpy.typing as npt
import xarray
from correlation_functions.correlation_statistics import CorrelationStatistics
from correlation_functions.main_structure import CorrelationFunction
from repository.repository_layer import RepositoryMetadata


class InputIterator:
    """The input iterator is responsible for receiving the respective input dataset files
    and facilitate the iteration over it
    """
    def _is_coord_scalar(self, dataset: xarray.Dataset) -> bool:
        return not self._repository_metadata.time_variation_dim in dataset.coords.indexes

    def _get_date_of_dataset_with_single_time_instance(self, dataset: xarray.Dataset) -> np.datetime64:
        return dataset.coords[self._repository_metadata.time_initial_dim].values + \
               dataset.coords[self._repository_metadata.time_variation_dim].values

    def __init__(self, input: List[xarray.Dataset], repository_metadata: RepositoryMetadata, 
        used_data_vars: List[str], input_time_intevals: Optional[List[int]],
        corr_function: Optional[CorrelationFunction] = None, 
        selection_params: Optional[Dict[str,Any]] = None) -> None:
        """Basic constructor to organize the dataset according to the time dimension

        Args:
            input (List[xarray.Dataset]): xarray Dataset objects reffering to the 
            input files
            repository_metadata (RepositoryMetadata): metadata repository
            used_data_vars (List[str]): name of data variables used
            input_time_intervals (Optional[List[int]]): number of steps that should exist
            between time instances or None, if it should not be used
            corr_function (Optional[CorrelationFunction]): the used correlation function
            in order to calculate the input statistics (or None if it is not necessary)
            selection_params (Optional[Dict[str,Any]]): if the 'corr_function' parameter
            is defined, this parameter must also be defined. This has the parameters 
            required to select the dimensions that should be used
        """
        self._repository_metadata: RepositoryMetadata = repository_metadata
        temp: List[Tuple[np.datetime64, xarray.Dataset]] = []
        for dataset in input:
            datetime_input: np.datetime64
            if self._is_coord_scalar(dataset):
                datetime_input = \
                    dataset.coords[repository_metadata.time_variation_dim].values + \
                    dataset.coords[repository_metadata.time_initial_dim].values
            else:
                datetime_input = \
                    dataset.coords[repository_metadata.time_variation_dim].values[0] + \
                    dataset.coords[repository_metadata.time_initial_dim].values

            temp.append((datetime_input, dataset))

        temp.sort(key=lambda x: x[0]) # type: ignore
        self._input: List[xarray.Dataset] = list(map(lambda x: x[1], temp))
        self._input_statistics: List[Tuple[xarray.Dataset, Dict[str,CorrelationStatistics]]] = [] #the keys of the dictionary are the data variables
        self._used_data_vars: List[str] = used_data_vars
        self._size_input:int = self._calculate_size()
        self._input_time_intervals: Optional[List[int]] = input_time_intevals

        if not corr_function is None and not selection_params is None:
            self._calculate_statistics(corr_function, selection_params,self._repository_metadata)
        elif corr_function is None and selection_params is None:
            pass
        else:
            raise ValueError("Either corr_function and selection_params are defined, or are both None")

    def _calculate_size(self) -> int:
        """Calculates the size of the input by time instances

        Returns:
            int: size of the input
        """
        res: int = 0
        date: np.datetime64
        for dataset in self._input:
            if self._is_coord_scalar(dataset):
                date = self._get_date_of_dataset_with_single_time_instance(dataset)
                if not self._repository_metadata.time_gap_container.is_gap(date, self._used_data_vars):
                    res += 1
                else:
                    logging.warning("Skipped date value " + str(date) + " as it represents a gap in the dataset")
            else:
                date = dataset.coords[self._repository_metadata.time_initial_dim].values #type: ignore

                steps: npt.NDArray[np.timedelta64] = \
                    dataset.coords[self._repository_metadata.time_variation_dim].values

                for step in steps:
                    if not self._repository_metadata.time_gap_container.is_gap(date + step, self._used_data_vars):
                        res += 1
                    else:
                        logging.warning("Skipped date value " + str(date) + " as it represents a gap in the dataset")
        return res
    
    
    def _calculate_statistics(self, corr_function: CorrelationFunction, selection_params: Dict[str,Any], 
        repository_metadata:RepositoryMetadata) -> None:
        """Calculates the statistics of the input files

        Args:
            corr_function (CorrelationFunction): correlation function that should be used to calculate the statistics
            selection_params (Dict[str,Any]): parameters that are going to be used to create the candidate (no longer used)
            repository_metadata (RepositoryMetadata): metadata of the repository
        """
        var_stats: Dict[str, CorrelationStatistics] = {}
        for dataset_full in self._input:
            if self._is_coord_scalar(dataset_full):
                var_stats = {}
                for var in self._used_data_vars:
                    var_stats[var] = corr_function.setup_stats(dataset_full.to_array(),repository_metadata,var)

                self._input_statistics.append(
                    (dataset_full.sel(selection_params),var_stats))
            else:
                steps: npt.NDArray[np.timedelta64] = \
                    dataset_full.coords[self._repository_metadata.time_variation_dim].values
                for step in steps:
                    params: Dict[str,Any] = {}
                    var_stats = {}
                    params[self._repository_metadata.time_variation_dim] = step
                    
                    for var in self._used_data_vars:
                        var_stats[var] = corr_function.setup_stats(dataset_full.sel(params).to_array(),repository_metadata,var)

                    self._input_statistics.append(
                        (dataset_full.sel(params).sel(selection_params),var_stats))

    def _calculate_next_time_interval(self, time_interval_index:int) -> Tuple[int,int]:
        """
        Calculates the next index and time_interval_value required

        Args:
            time_interval_index (int): current time interval index

        Returns:
            Tuple[int,int]: a pair of the new time_interval_index and time_interval_value
        """
        time_interval_value: int
        if self._input_time_intervals is None or\
           time_interval_index == 0 or\
           time_interval_index > len(self._input_time_intervals):
            time_interval_value = 0
        else:
            time_interval_value = self._input_time_intervals[time_interval_index-1]
        time_interval_index += 1
        return time_interval_index, time_interval_value

    def iterate(self) -> Iterator[Tuple[xarray.Dataset,int]]:
        """Iterator that allows you to interate the group of input files.

        Yields:
            Iterator[Tuple[xarray.Dataset,int]]: pair with the intended Dataset object and the 
            interval of time in numbers of timestamp between the returned timestamp and the next timestamp
        """
        date: np.datetime64
        time_interval_index: int = 0
        time_interval_value: int = 0

        for dataset in self._input:
            if self._is_coord_scalar(dataset):
                date = self._get_date_of_dataset_with_single_time_instance(dataset)
                if not self._repository_metadata.time_gap_container.is_gap(date, self._used_data_vars):
                    time_interval_index, time_interval_value = self._calculate_next_time_interval(time_interval_index)
                    yield (dataset, time_interval_value)
                else:
                    logging.warning("Skipped date value " + str(date) + " as it represents a gap in the dataset")
            else:
                steps: npt.NDArray[np.timedelta64] = \
                    dataset.coords[self._repository_metadata.time_variation_dim].values
                init_value: np.datetime64 = \
                    dataset.coords[self._repository_metadata.time_initial_dim].values #type: ignore
                for step in steps:
                    params: Dict[str,Any] = {}
                    params[self._repository_metadata.time_variation_dim] = step
                    
                    date = init_value + step
                    if not self._repository_metadata.time_gap_container.is_gap(date,self._used_data_vars):
                        time_interval_index, time_interval_value = self._calculate_next_time_interval(time_interval_index)
                        yield (dataset.sel(params), time_interval_value)
                    else:
                        logging.warning("Skipped date value " + str(date) + " as it represents a gap in the dataset")

    def iterate_with_statistics(self) -> Iterator[Tuple[xarray.Dataset, Dict[str,CorrelationStatistics],int]]:
        """Iterates the input files together with its respective statistics object

        Yields:
            Iterator[Tuple[xarray.Dataset, Dict[str,CorrelationStatistics],int]]: the Dataset object together
            with its statistics and the time gap in number timestamps from the next input file
        """
        date: np.datetime64
        time_interval_index: int = 0
        time_interval_value: int = 0

        for ds_stats_pair in self._input_statistics:
            if self._is_coord_scalar(ds_stats_pair[0]):
                date = self._get_date_of_dataset_with_single_time_instance(ds_stats_pair[0])
                if not self._repository_metadata.time_gap_container.is_gap(date, self._used_data_vars):
                    time_interval_index, time_interval_value = self._calculate_next_time_interval(time_interval_index)
                    yield (ds_stats_pair[0], ds_stats_pair[1], time_interval_value)
                else:
                    logging.warning("Skipped date value " + str(date) + " as it represents a gap in the dataset")
            else:
                steps: npt.NDArray[np.timedelta64] = \
                    ds_stats_pair[0].coords[self._repository_metadata.time_variation_dim].values
                init_value: np.datetime64 = \
                    ds_stats_pair[0].coords[self._repository_metadata.time_initial_dim].values #type: ignore
                for step in steps:
                    params: Dict[str,Any] = {}
                    params[self._repository_metadata.time_variation_dim] = step
                    date = init_value + step
                    if not self._repository_metadata.time_gap_container.is_gap(date,self._used_data_vars):
                        time_interval_index, time_interval_value = self._calculate_next_time_interval(time_interval_index)
                        yield (ds_stats_pair[0].sel(params), ds_stats_pair[1], time_interval_value)
                    else:
                        logging.warning("Skipped date value " + str(date) + " as it represents a gap in the dataset")

    @property
    def size(self) -> int:
        return self._size_input

    def close(self) -> None:
        for dataset in self._input:
            dataset.close()


class ResultContainer:
    """Container for the similarity results
    """

    def __init__(self, value: float):
        self._value: float = value
        self._sum_counter: int = 1

    def add_value(self, value: float) -> None:
        self._value += value
        self._sum_counter += 1

    @property
    def value(self) -> float:
        return self._value

    @property
    def sum_counter(self) -> int:
        return self._sum_counter

    def __repr__(self) -> str:
        return "{ Value: " + str(self._value) + \
        ", Sum Counter: " + str(self._sum_counter) + "}"

class CandidateContainer:
    """Container for the similarity range results
    Used to track the current accumulated similarity value, but also
    how many instances have been added, so it can be latter divided
    """

    def __init__(self, best_value: float, worst_value: float, data_vars:List[str]) -> None:
        self._best_value: float = best_value
        self._worst_value: float = worst_value
        self._sum_counter: Dict[str,int] = {}
        for var in data_vars:
            self._sum_counter[var] = 1
    
    def add_value(self, best_value: float, worst_value: float, data_vars:List[str], sum_counter: int = 1) -> None:
        """Sums the calculated candidate value into the already existing values

        Args:
            best_value (float): newly obtained best value
            worst_value (float): newly obtained worst value
            data_vars (List[str]): data variables it refers to
            sum_counter (int, optional): Number of times the interval should count. Defaults to 1.
        """
        self._best_value += best_value
        self._worst_value += worst_value
        for var in data_vars:
            if var in self._sum_counter:
                self._sum_counter[var] += 1
            else:
                self._sum_counter[var] = 1

    def add_value_without_increasing_counter(self,best_value: float, worst_value: float) -> None:
        """To add a newly calculated value without without increasing the internal counter
        (used to divide the sum of all values to obtain the actual candidate value)

        Args:
            best_value (float): newly obtained best value
            worst_value (float): newly obtained worst value
        """
        self._best_value += best_value
        self._worst_value += worst_value

    @property
    def best_value(self) -> float:
        return self._best_value

    @property
    def worst_value(self) -> float:
        return self._worst_value

    @property
    def sum_counter(self) -> int:
        value: int = -1
        for var in self._sum_counter:
            if value < 0:
                value = self._sum_counter[var]
            elif value != self._sum_counter[var]:
                raise ValueError("Sums of data variables in CandidateHolder do not match. Should be equal")
        if value < 0:
            raise ValueError("Sum Counter is empty")
        return value

    def is_final(self) -> bool:
        return self._sum_counter == 0

    def set_as_final(self) -> None:
        """Calculate the final value of the candidate

        Raises:
            ValueError: If the candidate was already set to final
        """
        if self._sum_counter != 0:
            self._best_value /= self.sum_counter
            self._worst_value /= self.sum_counter
            self._sum_counter = {}
        else:
            raise ValueError("CandidateContainer has already been set as final")

    def __repr__(self) -> str:
        return "{ Best Value: "  + str(self._best_value) + \
               ", Worst Value: " + str(self._worst_value) + \
               ", Sum Counter: " + str(self._sum_counter) + "}"

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, CandidateContainer):
            return __o.best_value == self._best_value and \
                   __o.worst_value == self._worst_value and \
                   __o.sum_counter == self._sum_counter
        else:
            return False


class CandidateListManager:
    """Class responsible for managing the existing candidate list"""

    def __init__(self, used_correlation_function: CorrelationFunction, top_res: int) -> None:
        # structure of the elements of the list: timestamp, best value, worst value
        self._list: List[Tuple[np.datetime64, CandidateContainer]] = []
        self._corr_func: CorrelationFunction = used_correlation_function
        self._top_res: int = top_res

    def _pop_if_worst_interval(self) -> bool:
        """Removes the last element in the list if it's best value is worse
        than the worst value in the "top_res" position 

        Returns:
            bool: True if the item was removed
        """
        if len(self._list) <= self._top_res:
            return False
        else:
            last_elem_best_val: float = self._list[-1][1].best_value
            top_res_n_worst_val: float = self._list[self._top_res-1][1].worst_value
            if self._corr_func.compare(last_elem_best_val, top_res_n_worst_val) and \
                last_elem_best_val != top_res_n_worst_val:
                self._list.pop()
                return True
            else:
                return False
    
    def _insert_sorted(self, value: Tuple[np.datetime64, CandidateContainer]) -> None:
        """Inserts the element inside the list of candidates and sorts it by
        worst similarity value

        Args:
            value (Tuple[np.datetime64, float, float]): value to be inserted
        """
        self._list.append(value)
        for i in range(len(self._list)-1, 0, -1):
            if self._corr_func.compare(self._list[i-1][1].worst_value,self._list[i][1].worst_value):
                self._list[i-1],self._list[i] = self._list[i],self._list[i-1]
            else:
                break

        for i in range(len(self._list), self._top_res, -1):
            if not self._pop_if_worst_interval():
                break

    def add_value(self, datetime: np.datetime64, candidate_container: CandidateContainer) -> None:
        """Adds the value to the list of candidates, if it should be added

        Args:
            datetime (np.datetime64): datetime it refferes to
            best_value (float): best calculated similarity value
            worst_value (float): worst calculated similarity value
        """
        if len(self._list) < self._top_res:
            self._insert_sorted((datetime,candidate_container))
        else:
            top_res_n_worst_val: float = self._list[self._top_res-1][1].worst_value
            if self._corr_func.compare(top_res_n_worst_val, candidate_container.best_value) or \
                top_res_n_worst_val == candidate_container.best_value:
                self._insert_sorted((datetime,candidate_container))

    def get_results(self) -> Iterator[np.datetime64]:
        """Returns the datetime values remaining in the list of candidates

        Yields:
            Iterator[np.datetime64]: Iterator with the existing values
        """
        for elem in self._list:
            yield elem[0]

    def to_dict(self) -> Dict[np.datetime64, CandidateContainer]:
        res: Dict[np.datetime64, CandidateContainer] = {}
        for elem in self._list:
            res[elem[0]] = elem[1]
        return res


    def __str__(self) -> str:
        res: str = "Obtained results:\n"

        for ts in self._list:
            res += str(ts) + "\n"
        res += "Total_size: " + str(len(self._list))
        
        return res