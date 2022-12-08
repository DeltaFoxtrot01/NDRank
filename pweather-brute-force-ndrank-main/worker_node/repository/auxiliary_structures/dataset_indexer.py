import datetime
import logging
import re
from typing import Callable, Dict, Final, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import yaml

SETTINGS: Final = "settings"
MONTH_YEAR_DATASET: Final = "month-year-dataset"
MONTH_YEAR_FILE_REGEX: Final = "ERA5-[0-9]{1,2}-[0-9]{4}.(nc|grib)"
HOUR_DAY_MONTH_YEAR_DATASET: Final = "hour-day-month-year-dataset"
HOUR_DAY_MONTH_YEAR_FILE_REGEX: Final = "ERA5-[0-9]{1,2}-[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}.(nc|grib)"


class DateContainer:
    """Object that is used to provide a timestamp as an argument. """
    
    def __init__(self, year:Optional[int] = None, month:Optional[int] = None, 
                       day: Optional[int] = None, hour:Optional[int] = None) -> None:
        self._year:Union[int, None] = year
        self._month:Union[int, None] = month
        self._day:Union[int, None] = day
        self._hour:Union[int, None] = hour

    def __hash__(self) -> int:
        res: int = 0
        if not self._year is None:
            res = self._year
        if not self._month is None:
            res = res * 100 + self._month
        if not self._day is None:
            res = res * 100 + self._day
        if not self._hour is None:
            res = res * 100 + self._hour
        return res

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, DateContainer):
            return hash(self) == hash(__o)
        else:
            return False

    @property
    def year(self) -> int:
        if self._year is None:
            raise ValueError("Value for year is None")
        return self._year
    
    @year.setter
    def year(self, year: int) -> None:
        self._year = year

    @property
    def month(self) -> int:
        if self._month is None:
            raise ValueError("Value for month is None")
        return self._month

    @month.setter
    def month(self, month: int) -> None:
        self._month = month

    def has_month(self) -> bool:
        return not self._month is None

    @property
    def day(self) -> int:
        if self._day is None:
            raise ValueError("Value for day is None")
        return self._day

    @day.setter
    def day(self, day: int) -> None:
        self._day = day

    def has_day(self) -> bool:
        return not self._day is None

    def unset_day(self) -> None:
        """Unsets the day attribute"""
        self._day = None
        
    @property
    def hour(self) -> int:
        if self._hour is None:
            raise ValueError("Value for hour is None")
        return self._hour

    @hour.setter
    def hour(self, hour: int) -> None:
        self._hour = hour

    def unset_hour(self) -> None:
        """Unsets the hour attribute"""
        self._hour = None

    def has_hour(self) -> bool:
        return not self._hour is None

    def to_datetime64(self)-> np.datetime64:
        """Converts DateContainer into datetime64

        Raises:
            ValueError: If year is not defined

        Returns:
            np.datetime64: equivalent value in datetime64
        """
        month: int = 1
        day: int = 1
        hour: int = 0
        if self._year is None:
            raise ValueError("Year must be defined")
        if not self._month is None:
            month = self._month
        if not self._day is None:
            day = self._day
        if not self._hour is None:
            hour = self._hour
        
        date: datetime.datetime = datetime.datetime(self._year, month, day, hour)
        return np.datetime64(date)

    def __repr__(self) -> str:
        return "Year: " + str(self._year) + "\tMonth: " + str(self._month) + "\tDay: " + str(self._day) + "\tHour: " + str(self._hour) 


def subtract_timedelta(datecontainer: DateContainer, timedelta: np.timedelta64) -> DateContainer:
    """Returns a new DateContainer with an updated value subtracting the
    timedelta
    Args:
        timedelta (np.timedelta64): timedelta to be subtracted
    Returns:
        DateContainer: the resulting DateContainer
    """
    datetime64_res: np.datetime64 = datecontainer.to_datetime64() - timedelta
    datetime_res: datetime.datetime = pd.to_datetime(datetime64_res)
    year: int = datetime_res.year
    month:Optional[int] = datetime_res.month if datecontainer.has_month() else None
    day:Optional[int] = datetime_res.day if datecontainer.has_day() else None
    hour:Optional[int] = datetime_res.hour if datecontainer.has_hour() else None
    return DateContainer(year,month,day,hour)
    

def _month_year_dataset(file_name: str) -> Tuple[int, DateContainer]:
    """
    Accepts file names in the following format:
    ERA5-<month>-<year>.nc

    And returns an integer value using the following formula:
    year*100 + month

    Args:
        file_name (str): file name

    Raises:
        ValueError: if the file in a invalid type

    Returns:
        int: calculated integer value
    """
    if re.fullmatch(MONTH_YEAR_FILE_REGEX,file_name) is None:
        raise ValueError("File is not in a valid format: ERA5-<month>-<year>.nc")

    filtered_str: List[str] = \
        file_name.replace("ERA5-","").replace(".nc","").replace(".grib","").split("-")
    
    year: int = int(filtered_str[1])
    month: int = int(filtered_str[0])

    return  year * 100 + month, DateContainer(year, month)

def _hour_day_month_year_dataset(file_name: str) -> Tuple[int, DateContainer]:
    """
    Accepts file names in the following format:
    ERA5-<hour>-<day>-<month>-<year>.nc

    And returns an integer value using the following formula:
    year*10 + month

    Args:
        file_name (str): file name

    Raises:
        ValueError: if the file in a invalid type

    Returns:
        int: calculated integer value
    """
    if re.fullmatch(HOUR_DAY_MONTH_YEAR_FILE_REGEX,file_name) is None:
        raise ValueError("File is not in a valid format: ERA5-<year>-<month>.nc")

    filtered_str: List[str] = \
        file_name.replace("ERA5-","").replace(".nc","").split("-")
    
    year: int = int(filtered_str[3])
    month: int = int(filtered_str[2])
    day: int = int(filtered_str[1])
    hour: int = int(filtered_str[0])

    return  ((year * 100 + month) * 100 + day) * 100 + hour, DateContainer(year, month, day, hour)


PROCESSING_FUNCTIONS: Dict[str,Callable[[str],Tuple[int,DateContainer]]] = {
    MONTH_YEAR_DATASET: _month_year_dataset,
    HOUR_DAY_MONTH_YEAR_DATASET: _hour_day_month_year_dataset
}

class DatasetIndexer:
    """
    Responsible for efficiently index the position of every file
    that consistutes the dataset for a sequencial search
    """
        
    def __init__(self, settings_file_path: str,
                       dataset_path: str,
                       file_name_converter: Callable[[str], Tuple[int, DateContainer]]) -> None:
        """
        Stores the path of the existing files that constitute the dataset
        and indexes these same files

        Args:
            settings_file_path (str): path of the file with the info of the 
            existing files
            dataset_path (str): path to the dataset files
            file_name_converter (Callable[[str], int]): function that converts
            the file name to a number that can be used to sort these files
        """
        #(value used for sorting purposes, path of the file, container with date information)
        self._file_array: List[Tuple[int, str, DateContainer]] = [] 
        file_list: List[str]
        self._file_indexer: Dict[int,int] = {} #the key is the hash value of the DateContainer

        with open(settings_file_path, 'r') as f:
            file_list = yaml.safe_load(f)[SETTINGS]

        self._file_array = [(0,"",DateContainer())] * len(file_list)
        container: DateContainer

        for i in range(len(file_list)):
            value:int
            value, container = file_name_converter(file_list[i])
            self._file_array[i] = \
                (value, dataset_path + file_list[i],container)
        
        self._file_array.sort(key=lambda elem: elem[0])

        for i in range(len(self._file_array)):
            _, _, container = self._file_array[i]
            self._file_indexer[hash(container)] = i
    
    def get_sorted_file_paths(self) -> List[Tuple[int,str,DateContainer]]:
        """
        Returns the exiting files sorted by the time they 
        represent

        Returns:
            List[Tuple[int,str]]: list of files, second parameter has
            the file path
        """
        return self._file_array

    def index_dataset(self, date: DateContainer) -> Optional[Tuple[int,str,DateContainer]]:
        """Returns a specific file path for a given date

        Args:
            date (DateContainer): date to be indexed

        Returns:
            Tuple[int,str,DateContainer]: found element of None
        """
        if not hash(date) in self._file_indexer:
            return None
        index: int = self._file_indexer[hash(date)]
        return self._file_array[index]

    def get_random_file(self) -> Tuple[int, str, DateContainer]:
        """Returns a random existing file. Expected to be used in a manner 
        that allows the analysis of characteristics that exist in all files.

        Returns:
            Tuple[int, str, DateContainer]: information related to the file
        """
        return self._file_array[0]

