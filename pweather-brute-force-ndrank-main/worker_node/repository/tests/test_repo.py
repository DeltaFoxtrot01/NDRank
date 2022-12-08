from sqlite3 import Timestamp
from typing import Tuple, Union
import pytest
import pandas as pd
import xarray
from repository.auxiliary_structures.dataset_indexer import DateContainer
from repository.implementations.month_year_repo import HourDayMonthYearRepository, MonthYearRepository


def test_successfull_dataset_iteration() -> None:
    repo: MonthYearRepository = \
        MonthYearRepository("./worker_node/testing_dataset",
                             "settings.yaml")

    prev_value: int = 0
    for file_index in repo.get_dataset():
        date: pd.Timestamp = \
            pd.to_datetime(file_index[1].coords['time'].values) #type: ignore
        m: int = date.month
        y: int = date.year
        assert prev_value < y * 100 + m
        prev_value = y * 100 + m
        file_index[1].close()

def test_successfull_dataset_iteration_hour_split() -> None:
    repo: HourDayMonthYearRepository = \
        HourDayMonthYearRepository("./worker_node/testing_dataset_days",
                             "settings.yaml")

    prev_value: int = 0
    for file_index in repo.get_dataset():
        date: pd.Timestamp = \
            pd.to_datetime(file_index[1].coords['time'].values) #type: ignore

        m: int = date.month
        y: int = date.year
        d: int = date.day
        h: int = date.hour                
        assert ((prev_value < y * 100 + m) * 100 + d) * 100 + h
        prev_value = ((prev_value < y * 100 + m) * 100 + d) * 100 + h
        file_index[1].close()


def test_access_to_specific_files() -> None:
    repo: MonthYearRepository = \
        MonthYearRepository("./worker_node/testing_dataset",
                             "settings.yaml")
    
    res: Union[None, Tuple[str, xarray.Dataset]] = repo.get_dataset_part(DateContainer(1980, 1))
    assert not (res is None)
    res[1].close()
    file_name: str = res[0].split("/")[-1]
    assert file_name == "ERA5-1-1980.nc"

def test_access_to_specific_files_hour_day() -> None:
    repo: HourDayMonthYearRepository = \
        HourDayMonthYearRepository("./worker_node/testing_dataset_days",
                                   "settings.yaml")
    
    res: Union[None, Tuple[str, xarray.Dataset]] = repo.get_dataset_part(DateContainer(1980, 1,3,6))
    assert not (res is None)
    res[1].close()
    file_name: str = res[0].split("/")[-1]
    assert file_name == "ERA5-6-3-1-1980.nc"

    res = repo.get_dataset_part(DateContainer(1980, 1, 8, 12))
    assert not (res is None)
    res[1].close()
    file_name = res[0].split("/")[-1]
    assert file_name == "ERA5-12-8-1-1980.nc"

def test_invalid_args() -> None:
    with pytest.raises(ValueError) as exec_info:
        MonthYearRepository("","")
    with pytest.raises(ValueError) as exec_info:
        MonthYearRepository("","asldjf")
    with pytest.raises(ValueError) as exec_info:
        MonthYearRepository("asdfdasf","")