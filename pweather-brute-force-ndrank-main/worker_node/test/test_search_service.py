import logging
import numpy
import pytest
import pickle
from typing import Any, BinaryIO, Dict, Tuple

import xarray
from repository.implementations.month_year_repo import MonthYearRepository
from repository.repository_collection import RepositoryCollection

from repository.repository_layer import RepositoryLayer, RepositoryMetadata
from service.data_types import CandidateContainer, ResultContainer
from service.implementations.brute_force_service import BruteForceService
from service.implementations.global_data_var_candidate_list_service import DataVarCandidateListService
from service.service_main_structure import DatasetSelectionParameter, HeuristicResult, RequestParameters, ServiceLayer
from correlation_functions.correlation_statistics import CorrelationStatistics
from correlation_functions.main_structure import CorrelationFunction

class TestPcc(CorrelationFunction):
    """Test correlation function
    """

    @property
    def max_value(self) -> float:
        return 1
    
    @property
    def min_value(self) -> float:
        return -1

    def calculate(self, dataarray1: xarray.DataArray, dataarray2: xarray.DataArray, repository_metadata: RepositoryMetadata, variable: str) -> float:
        return float(xarray.corr(dataarray1,dataarray2).data.item())
    
    def is_reverse_order(self) -> bool:
        return True
    
    def compare(self, value1: float, value2: float) -> bool:
        return value1 < value2
    
    def setup_stats(self, dataarray: xarray.DataArray, repo_metadata: RepositoryMetadata, variable: str) -> CorrelationStatistics:
        return CorrelationStatistics({})

    def calculate_partial_value(self, input_array: xarray.DataArray, dataset_array: xarray.DataArray, 
        input_stats: CorrelationStatistics, dataset_stats: CorrelationStatistics,
        selection_params: Dict[str,Any], repo_metadata: RepositoryMetadata, var: str) -> Tuple[float, float]:
        value: float = xarray.corr(dataset_array,input_array).data.item()
        return (value, value)

def test_simple_brute_force_search() -> None:
    repository: RepositoryLayer = \
        MonthYearRepository("./worker_node/testing_dataset","settings.yaml")
    service: ServiceLayer = \
        BruteForceService([repository])

    request_params: RequestParameters = RequestParameters()
    request_params.search_data_var = ["z"]
    res: Dict[str,ResultContainer] = service.execute_search(
            {
                "z":
                    ["./worker_node/testing_input_2/1980-01-03T06:00:00.000000000.nc", 
                     "./worker_node/testing_input_2/1980-01-03T12:00:00.000000000.nc"]
            },
                request_params,
                TestPcc("pcc")
            )[0]
    file: BinaryIO = open("./worker_node/test/simple_search_res.bin","rb")
    stored_res: Dict[str,float] = pickle.load(file)
    file.close()

    for key in stored_res:
        assert key in res
        assert stored_res[key] == res[key].value

    assert len(res) == len(stored_res)

def test_simple_brute_force_search_with_two_vars() -> None:
    repository: RepositoryLayer = \
        MonthYearRepository("./worker_node/testing_dataset","settings.yaml")
    service: ServiceLayer = \
        BruteForceService([repository])

    request_params: RequestParameters = RequestParameters()
    request_params.search_data_var = ["z","t"]
    res: Dict[str,ResultContainer] = service.execute_search(
        {
            "z":
                ["./worker_node/testing_input_2/1980-01-03T06:00:00.000000000.nc", 
                 "./worker_node/testing_input_2/1980-01-03T12:00:00.000000000.nc"],
            "t":
                ["./worker_node/testing_input_2/1980-01-03T06:00:00.000000000.nc", 
                 "./worker_node/testing_input_2/1980-01-03T12:00:00.000000000.nc"]
        },
                request_params,
                TestPcc("pcc")
            )[0]
    file: BinaryIO = open("./worker_node/test/two_var_search_res.bin","rb")
    stored_res: Dict[str,float] = pickle.load(file)
    file.close()

    for key in stored_res:
        assert key in res
        assert round(stored_res[key],12) == round(res[key].value,12)

    assert len(res) == len(stored_res)

def test_simple_brute_force_search_with_two_vars_double_repo() -> None:
    repository: RepositoryLayer = \
        MonthYearRepository("./worker_node/testing_dataset","settings1.yaml")
    repository2: RepositoryLayer = \
        MonthYearRepository("./worker_node/testing_dataset","settings2.yaml")
    service: ServiceLayer = \
        BruteForceService([repository, repository2])

    request_params: RequestParameters = RequestParameters()
    request_params.search_data_var = ["z","t"]
    res: Dict[str,ResultContainer] = service.execute_search(
        {
            "z":
                ["./worker_node/testing_input_2/1980-01-03T06:00:00.000000000.nc", 
                 "./worker_node/testing_input_2/1980-01-03T12:00:00.000000000.nc"],
            "t":
                ["./worker_node/testing_input_2/1980-01-03T06:00:00.000000000.nc", 
                 "./worker_node/testing_input_2/1980-01-03T12:00:00.000000000.nc"]
        },
                request_params,
                TestPcc("pcc")
            )[0]
    file: BinaryIO = open("./worker_node/test/two_var_search_res.bin","rb")
    stored_res: Dict[str,float] = pickle.load(file)
    file.close()

    for key in stored_res:
        assert key in res
        assert round(stored_res[key],12) == round(res[key].value,12)

    assert len(res) == len(stored_res)

def test_specific_timestamps_brute_force_search_two_vars() -> None:
    repository: RepositoryLayer = \
        MonthYearRepository("./worker_node/testing_dataset","settings.yaml")
    service: ServiceLayer = \
        BruteForceService([repository])

    request_params: RequestParameters = RequestParameters()
    request_params.search_data_var = ["z","t"]
    request_params.ts_neighbour_gap = 1
    res: Dict[str,ResultContainer] = service.execute_search_on_ts(iter(
                [
                    HeuristicResult('1980-01-01T06:00:00.000000000',1.0),
                    HeuristicResult('1980-01-09T12:00:00.000000000',1.0),
                    HeuristicResult('1980-01-31T18:00:00.000000000',1.0)
                ]),
                {
                    "z":
                        ["./worker_node/testing_input_2/1980-01-03T06:00:00.000000000.nc", 
                         "./worker_node/testing_input_2/1980-01-03T12:00:00.000000000.nc"],
                    "t":
                        ["./worker_node/testing_input_2/1980-01-03T06:00:00.000000000.nc", 
                         "./worker_node/testing_input_2/1980-01-03T12:00:00.000000000.nc"]
                },
                request_params,
                TestPcc("pcc")
            )[0]

    file: BinaryIO = open("./worker_node/test/two_var_search_res.bin","rb")
    stored_res: Dict[str,float] = pickle.load(file)
    file.close()

    for key in res:
        print(key)
        assert key in stored_res
        assert stored_res[key] == res[key].value

def test_specific_timestamps_brute_force_search_to_vars_two_repos() -> None:
    repository: RepositoryLayer = \
        MonthYearRepository("./worker_node/testing_dataset","settings1.yaml")
    repository2: RepositoryLayer = \
        MonthYearRepository("./worker_node/testing_dataset","settings2.yaml")
    service: ServiceLayer = \
        BruteForceService([repository, repository2])

    request_params: RequestParameters = RequestParameters()
    request_params.search_data_var = ["z","t"]
    request_params.ts_neighbour_gap = 1
    res: Dict[str,ResultContainer] = service.execute_search_on_ts(iter(
                [
                    HeuristicResult('1980-01-01T06:00:00.000000000',1.0),
                    HeuristicResult('1980-01-09T12:00:00.000000000',1.0),
                    HeuristicResult('1980-01-31T18:00:00.000000000',1.0)
                ]),
                {
                    "z":
                        ["./worker_node/testing_input_2/1980-01-03T06:00:00.000000000.nc", 
                         "./worker_node/testing_input_2/1980-01-03T12:00:00.000000000.nc"],
                    "t":
                        ["./worker_node/testing_input_2/1980-01-03T06:00:00.000000000.nc", 
                         "./worker_node/testing_input_2/1980-01-03T12:00:00.000000000.nc"]
                },
                request_params,
                TestPcc("pcc")
            )[0]

    file: BinaryIO = open("./worker_node/test/two_var_search_res.bin","rb")
    stored_res: Dict[str,float] = pickle.load(file)
    file.close()

    for key in res:
        assert key in stored_res
        assert stored_res[key] == res[key].value

def test_candidate_data_var_list_brute_force_search_to_vars() -> None:
    repository: RepositoryLayer = \
        MonthYearRepository("./worker_node/testing_dataset","settings.yaml")
    service: ServiceLayer = \
        DataVarCandidateListService([repository])

    request_params: RequestParameters = RequestParameters()
    request_params.search_data_var = ["z","t"]
    request_params.ts_neighbour_gap = 1
    request_params.selection_data_vars = ["z"]

    res: Dict[numpy.datetime64,CandidateContainer] = service.execute_search_for_candidates(
                {
                    "z":
                        ["./worker_node/testing_input_2/1980-01-03T06:00:00.000000000.nc", 
                         "./worker_node/testing_input_2/1980-01-03T12:00:00.000000000.nc"],
                    "t":
                        ["./worker_node/testing_input_2/1980-01-03T06:00:00.000000000.nc", 
                         "./worker_node/testing_input_2/1980-01-03T12:00:00.000000000.nc"]
                },
                request_params,
                TestPcc("pcc"),
                1000000
            )[0]
    file: BinaryIO = open("./worker_node/test/best_param_search_res.bin","rb")
    best_stored_res: Dict[str,float] = pickle.load(file)
    file.close()

    file = open("./worker_node/test/worst_param_search_res.bin","rb")
    worst_stored_res: Dict[str,float] = pickle.load(file)
    file.close()

    for key in res:
        assert str(key) in best_stored_res
        assert str(key) in worst_stored_res

        assert round(best_stored_res[str(key)],12) == round(res[key].best_value,12)
        assert round(worst_stored_res[str(key)],12) == round(res[key].worst_value,12)

def test_candidate_data_var_list_brute_force_search_to_vars_two_repos() -> None:
    repository: RepositoryLayer = \
        MonthYearRepository("./worker_node/testing_dataset","settings1.yaml")
    repository2: RepositoryLayer = \
        MonthYearRepository("./worker_node/testing_dataset","settings2.yaml")
    service: ServiceLayer = \
        DataVarCandidateListService([repository,repository2])

    request_params: RequestParameters = RequestParameters()
    request_params.search_data_var = ["z","t"]
    request_params.ts_neighbour_gap = 1
    request_params.selection_data_vars = ["z"]

    res: Dict[numpy.datetime64,CandidateContainer] = service.execute_search_for_candidates(
                {
                    "z":
                        ["./worker_node/testing_input_2/1980-01-03T06:00:00.000000000.nc", 
                         "./worker_node/testing_input_2/1980-01-03T12:00:00.000000000.nc"],
                    "t":
                        ["./worker_node/testing_input_2/1980-01-03T06:00:00.000000000.nc", 
                         "./worker_node/testing_input_2/1980-01-03T12:00:00.000000000.nc"]
                },
                request_params,
                TestPcc("pcc"),
                1000000
            )[0]
    file: BinaryIO = open("./worker_node/test/best_param_search_res.bin","rb")
    best_stored_res: Dict[str,float] = pickle.load(file)
    file.close()

    file = open("./worker_node/test/worst_param_search_res.bin","rb")
    worst_stored_res: Dict[str,float] = pickle.load(file)
    file.close()

    for key in res:
        assert str(key) in best_stored_res
        assert str(key) in worst_stored_res

        assert round(best_stored_res[str(key)],12) == round(res[key].best_value,12)
        assert round(worst_stored_res[str(key)],12) == round(res[key].worst_value,12)

def test_brute_force_search_with_interval_distance() -> None:
    repository: RepositoryLayer = \
        MonthYearRepository("./worker_node/testing_dataset","settings.yaml")
    service: ServiceLayer = \
        BruteForceService([repository])

    request_params: RequestParameters = RequestParameters()
    request_params.search_data_var = ["z"]
    request_params.input_step_difference = [10]
    res: Dict[str,ResultContainer] = service.execute_search(
            {

                "z":
                    ["./worker_node/testing_input_2/1980-01-03T06:00:00.000000000.nc", 
                     "./worker_node/testing_input_2/1980-01-03T12:00:00.000000000.nc",
                     "./worker_node/testing_input_2/1980-01-03T18:00:00.000000000.nc",
                     "./worker_node/testing_input_2/1980-01-04T00:00:00.000000000.nc"]
            },
                request_params,
                TestPcc("pcc")
            )[0]

    file: BinaryIO = open("./worker_node/test/10_hour_search_res.bin","rb")
    stored_res: Dict[str,float] = pickle.load(file)
    file.close()

    for key in res:
        assert key in stored_res and stored_res[key] == res[key].value
        

def test_brute_force_search_with_2_intervals_distance() -> None:
    repository: RepositoryLayer = \
        MonthYearRepository("./worker_node/testing_dataset","settings.yaml")
    service: ServiceLayer = \
        BruteForceService([repository])

    request_params: RequestParameters = RequestParameters()
    request_params.search_data_var = ["z"]
    request_params.input_step_difference = [0,10,10]
    res: Dict[str,ResultContainer] = service.execute_search(
                {
                    "z":
                        ["./worker_node/testing_input_2/1980-01-03T06:00:00.000000000.nc", 
                         "./worker_node/testing_input_2/1980-01-03T12:00:00.000000000.nc",
                         "./worker_node/testing_input_2/1980-01-03T18:00:00.000000000.nc",
                         "./worker_node/testing_input_2/1980-01-04T00:00:00.000000000.nc"]
                },
                request_params,
                TestPcc("pcc")
            )[0]

    file: BinaryIO = open("./worker_node/test/two_10_hour_search_res.bin","rb")
    stored_res: Dict[str,float] = pickle.load(file)
    file.close()

    for key in res:
        assert key in stored_res and stored_res[key] == res[key].value