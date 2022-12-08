import logging
import os
import subprocess
from datetime import datetime
import tempfile
from typing import Any, Dict, Final, Iterator, Optional, Tuple
from dask.diagnostics import ProgressBar

import numpy as np
import numpy.typing as npt
import pandas #type: ignore
import xarray
import yaml
from aux_functions.xarray_aux import open_dataarray_with_file_name, open_dataset_with_file_name
from resolution_reducer_tools.resolution_reducer import resolution_reducer_interface

NC_EXTENSION: Final = ".nc"
GRIB_EXTENSION: Final = ".grib"

def array_interval_average(source_file: str, interval: int, first_val_dim:str, variation_dim: str,
    previous_file: Optional[str] = None, next_file: Optional[str] = None) -> Iterator[Tuple[np.datetime64, np.timedelta64, xarray.DataArray]]:
    """
    This function obtains the timestamp from the "source_file" and averages it with the neighbouring 
    timestamps. This is used in scenarios where it is necessary to reduce the noise that may exist in 
    the data.

    Args:
        source_file (str): path of the file that is being analysed
        interval (int): number of neighbouring timestamps that should be searched
        first_val_dim (str): name of the time dimension that has the first time value
        variation_dim (str): name of the time dimension that has the variation values of the time instance
        previous_file (Optional[str]): file path of the previous month of source_file. Default is None
        next_file (Optional[str]): file path of the following month of source_file. Default is None

    Returns:
        Iterator[Tuple[np.datetime64, np.timedelta64, xarray.DataArray]]: it iterates every existing array in source_file with the 
        timestamp and the respective resulting data array value
    
    Raises:
        ValueError: if both the previous_file and next_file are None
    """
    if previous_file is None and next_file is None:
        raise ValueError("For this reducer, it is necessary to either have a past or future file")

    ds: xarray.Dataset = open_dataset_with_file_name(source_file)
    step_values: npt.NDArray[np.timedelta64] = ds.coords[variation_dim].values
    first_date: np.datetime64 = ds.coords[first_val_dim].values #type: ignore

    ds_past: Optional[xarray.Dataset] = None
    past_step_values: npt.NDArray[np.timedelta64]

    ds_future: Optional[xarray.Dataset] = None
    future_step_values: npt.NDArray[np.timedelta64]

    if not previous_file is None:
        ds_past = open_dataset_with_file_name(previous_file)
        past_step_values = ds_past.coords[variation_dim].values

        if not next_file is None:
            ds_future = open_dataset_with_file_name(next_file)
            future_step_values = ds_future.coords[variation_dim].values

    for index in range(len(step_values)):
        final_array: Optional[xarray.DataArray] = None
        num_arrays: int = 0
        """This part of the code is here to reduce the impact of possible
        anomalies in the code. This is done by first averaging the neighbouring
        days of the used timestamp
        """
        for index_pos in range(index - interval, index + interval + 1):
            current_array: Optional[xarray.DataArray]
            params: Dict[str, Any] = {}

            if index_pos < 0 and not ds_past is None:
                params[variation_dim] = past_step_values[len(past_step_values) + index_pos]
                current_array = ds_past.sel(params).to_array()
                num_arrays += 1
            elif index_pos >= len(step_values) and not ds_future is None:
                params[variation_dim] = future_step_values[index_pos - len(step_values)]
                current_array = ds_future.sel(params).to_array()
                num_arrays += 1
            elif 0 <= index_pos < len(step_values):
                params[variation_dim] = step_values[index_pos]
                current_array = ds.sel(params).to_array()
                num_arrays += 1
            else:
                current_array = None
            
            if not current_array is None:
                if final_array is None:
                    final_array = current_array
                else:
                    final_array += current_array

        yield (first_date, step_values[index], final_array/num_arrays)


def get_month_day_hour(datetime64: np.datetime64) -> Tuple[int,int,int]:
    d_t: datetime  = pandas.to_datetime(datetime64)
    return (d_t.month, d_t.day, d_t.hour)

class array_sum_pair:
    """Data structure that keeps the sum of the given data arrays, tracking the number of
    arrays that have been summed. 

    With this information, the final value can then be stored in a netcdf file
    """

    def __init__(self, data_array: xarray.DataArray, file_name: str) -> None:
        if not ".nc" in file_name:
            raise ValueError("File name does not have a valid extension, must be " + NC_EXTENSION)
        logging.debug("Given file name: " + file_name)
        self._counter:int = 1
        self._file_name: str = file_name
        self._temp_file_name: str = file_name.split(NC_EXTENSION)[0] + "_temp" + NC_EXTENSION
        logging.debug("Temporary file name: " + file_name)
        data_array.to_netcdf(self._file_name)
        data_array.close()

    def _open_array(self) -> xarray.DataArray:
        return open_dataarray_with_file_name(self._file_name)
    
    def _replace_array(self) -> None:
        error1: int = subprocess.call(["rm", self._file_name])
        error2: int = subprocess.call(["mv", self._temp_file_name, self._file_name])
        if error1 != 0 or error2 != 0:
            raise OSError("Error replacing array file")

    def sum_array(self, data_array: xarray.DataArray) -> None:
        array: xarray.DataArray = self._open_array()
        array += data_array
        self._counter += 1
        array.to_netcdf(self._temp_file_name)
        array.close()
        self._replace_array()

    def set_average(self) -> None:
        array: xarray.DataArray = self._open_array()
        array = array / self._counter
        array.to_netcdf(self._temp_file_name)
        array.close()
        self._replace_array()

    def set_average_and_calc_sqrt(self) -> None:
        array: xarray.DataArray = self._open_array()
        array = array / self._counter
        array = array ** (1/2)
        array.to_netcdf(self._temp_file_name)
        array.close()
        self._replace_array()

    def get_array(self) -> xarray.DataArray:
        return self._open_array()

    def get_path(self) -> str:
        return self._file_name
    
class container_day_average:
    """This data structure is responsible for receiving all the
    data arrays for each time instance and sum them together.
    After all calculations are finished, it calculates the average
    and stores them in netcdf files.
    """

    def __init__(self, path:str):
        #way that it is indexed: month, day, hour
        self._container: Dict[int, Dict[int, Dict[int, array_sum_pair]]] = {}
        self._path: str = path
        if self._path[-1] != "/":
            self._path += "/"

    def add_array(self, date: np.datetime64, time: np.timedelta64, array: xarray.DataArray, file_prefix: str = "AVERAGE") -> None:
        datetime64_current: np.datetime64 = date + time
        datetime_current: datetime = pandas.to_datetime(datetime64_current)
        month: int = datetime_current.month
        day: int = datetime_current.day
        hour: int = datetime_current.hour

        if not month in self._container:
            self._container[month] = {}
        
        if not day in self._container[month]:
            self._container[month][day] = {}

        if not hour in self._container[month][day]:
            file_name: str = self._path + file_prefix + "-" + str(month) + \
                 "-" + str(day) + "-" + str(hour) + ".nc"
            #logging.debug("Added " + str(month) + "-" + str(day) + "-" + str(hour) + " to the container")
            self._container[month][day][hour] = array_sum_pair(array, file_name)
        else:
            self._container[month][day][hour].sum_array(array)

    def set_average(self) -> None:
        structure_dictionary: Dict[int, Dict[int, Dict[int, str]]] = {}

        for month in self._container:
            for day in self._container[month]:
                for hour in self._container[month][day]:
                    self._container[month][day][hour].set_average()
                    if not month in structure_dictionary:
                        structure_dictionary[month] = {}            
                    if not day in structure_dictionary[month]:
                        structure_dictionary[month][day] = {}

                    structure_dictionary[month][day][hour] = self._container[month][day][hour].get_path()
        
        with open(self._path + "average.yaml", 'w') as infile:
            yaml.dump(structure_dictionary,infile)

    def set_standard_deviation(self) -> None:
        structure_dictionary: Dict[int, Dict[int, Dict[int, str]]] = {}

        for month in self._container:
            for day in self._container[month]:
                for hour in self._container[month][day]:
                    self._container[month][day][hour].set_average_and_calc_sqrt()
                    if not month in structure_dictionary:
                        structure_dictionary[month] = {}            
                    if not day in structure_dictionary[month]:
                        structure_dictionary[month][day] = {}

                    structure_dictionary[month][day][hour] = self._container[month][day][hour].get_path()
        
        with open(self._path + "standard_deviation.yaml", 'w') as infile:
            yaml.dump(structure_dictionary,infile)

    def index_array(self, month:int, day: int, hour: int) -> xarray.DataArray:
        return self._container[month][day][hour].get_array()


class year_average_reducer(resolution_reducer_interface):
    """Calculate the average of each month by timestamp, of all the available years
    """
    def __init__(self, temporary_folder: str, result_file: str, variation_dim: str, first_val_dim: str, interval: int):
        """
        Constructor for year_average_reducer

        Args:
            temporary_folder (str): folder to create temporary files
            result_file (str): folder with the final results
            variation_dim (str): time dimension that has the time values
            first_val_dim (str): dimension that has the first value existing in the time 
            interval (int): number of neighbouring timestamps for each side that should be 
            averaged with the current array
        """
        super().__init__(temporary_folder)
        self._result_file: str = result_file
        self._variation_dim: str = variation_dim
        self._first_val_dim: str = first_val_dim
        self._interval: int = interval
        self._container: container_day_average = \
            container_day_average(self._result_file)

    def reduce_resolution(self, source_file: str, file_name: str, previous_file: Optional[str] = None, 
        next_file: Optional[str] = None) -> Tuple[str, str]:

        for ts_array_pair in array_interval_average(source_file, self._interval, self._first_val_dim, 
                self._variation_dim, previous_file, next_file):
            self._container.add_array(ts_array_pair[0], ts_array_pair[1], ts_array_pair[2])

        return ("","")

    def clear_cached_file(self):
        pass

    def execute_at_end(self):
        self._container.set_average()

class year_standard_deviation_reducer(resolution_reducer_interface):
    """Reducer to calculate the standard deviation of all existing time 
    instances in a year. For this reducer, it is required to already have
    the average files created
    """

    def __init__(self, temporary_folder: str, result_file: str, average_path: str,variation_dim: str, first_val_dim: str, interval: int):
        """
        Constructor for year_standard_deviation_reducer

        Args:
            temporary_folder (str): folder to create temporary files
            result_file (str): folder with the final results
            average_path (str): path of the folder with the average results
            variation_dim (str): time dimension that has the time values
            first_val_dim (str): dimension that has the first value existing in the time dimension
            interval (int): number of neighbouring timestamps for each side that should be 
            averaged with the current array
        """
        super().__init__(temporary_folder)
        self._result_file: str = result_file
        self._average_path: str = average_path
        if self._average_path[-1] != "/":
            self._average_path += "/"
        self._variation_dim: str = variation_dim
        self._first_val_dim: str = first_val_dim
        self._interval: int = interval
        
        self._is_file_created: bool = False
        self._container: container_day_average = \
            container_day_average(self._result_file)
        self._average_container: Dict[int, Dict[int, Dict[int, str]]] #month, day, hour

    def _init_file(self) -> None:
        """Function to open the yaml file with all path for the average data arrays"""
        if not self._is_file_created:
            self._is_file_created
            with open(self._average_path + "average.yaml","r") as handler:
                self._average_container = yaml.safe_load(handler)

    def reduce_resolution(self, source_file: str, file_name: str, previous_file: Optional[str] = None, 
        next_file: Optional[str] = None) -> Tuple[str, str]:
        
        hour: int
        day: int
        month: int
        self._init_file()
        for ts_array_pair in array_interval_average(source_file, self._interval, self._first_val_dim, 
                self._variation_dim, previous_file, next_file):
            month, day, hour = get_month_day_hour(ts_array_pair[0] + ts_array_pair[1])
            avg_array: xarray.DataArray = open_dataarray_with_file_name(self._average_container[month][day][hour])

            self._container.add_array(ts_array_pair[0], ts_array_pair[1], (ts_array_pair[2] - avg_array)**2,"STANDARD-DEVIATION")

        return ("","")

    def execute_at_end(self):
        self._container.set_standard_deviation()

class anomaly_reducer(resolution_reducer_interface):
    """Uses the files created by the year_average_reducer and converts a dataset
    with the original values to a dataset with anomaly values.

    This is done by subtracting the original file by it's average

    """
    def __init__(self, temporary_folder: str, path_to_object_average: str, variation_dim: str, first_val_dim: str):
        super().__init__(temporary_folder)
        if path_to_object_average[-1] != "/":
            path_to_object_average += "/"

        self._file_path: str = path_to_object_average
        self._variation_dim: str = variation_dim
        self._first_val_dim: str = first_val_dim
        self._is_file_created: bool = False

    def _init_file(self) -> None:
        """Function to open the yaml file with all path for the average data arrays"""
        if not self._is_file_created:
            self._is_file_created
            with open(self._file_path + "average.yaml","r") as handler:
                self._container: Dict[int, Dict[int, Dict[int, str]]] = yaml.safe_load(handler)

    def reduce_resolution(self, source_file: str, file_name: str, previous_file: Optional[str] = None, 
        next_file: Optional[str] = None) -> Tuple[str, str]:
        extension: str
        if file_name.endswith(NC_EXTENSION):
            extension = NC_EXTENSION
        elif file_name.endswith(GRIB_EXTENSION):
            extension = GRIB_EXTENSION
        else:
            raise ValueError("File " + file_name + " does not have a known extension")

        file_descriptor, output_file = tempfile.mkstemp(prefix=file_name.split(extension)[0], 
                                                suffix=extension,
                                                dir=self._temporary_folder)

        #function is called here and not in the constructor, because the constructor is always
        #called in the __main__ file, even if the reducer is not reduced. Otherwise this would
        #force the existance of an .yaml file for a reducer that would not even be called
        self._init_file()
        error_code:int = subprocess.call(["cp",source_file,output_file])
        if error_code != 0:
            raise OSError("Unable to create copy of file " + source_file + ". Error code: " + str(error_code))
        
        os.close(file_descriptor)

        chunked: Dict[str, int] = {}
        chunked[self._variation_dim] = 1
        dataset: xarray.Dataset = open_dataset_with_file_name(output_file,chunked)

        first_date: np.datetime64 = dataset.coords[self._first_val_dim].values #type: ignore
        step_values: npt.NDArray[np.timedelta64] = dataset.coords[self._variation_dim].values

        month: int
        day: int
        hour: int
        logging.debug("Subtracting timestamps")
        for step_index in range(len(step_values)):
            month, day, hour = \
                get_month_day_hour(first_date + step_values[step_index])
            average_array: xarray.DataArray = open_dataarray_with_file_name(self._container[month][day][hour])
            params: Dict[str, Any] = {}
            params[self._variation_dim] = step_index
            average_dataset: xarray.Dataset = average_array.to_dataset(dim="variable")
            dataset[params] -= average_dataset
            average_dataset.close()
            average_array.close()

        logging.debug("creating copy and storing in netcdf format")
        file_descriptor, netcdf_file = tempfile.mkstemp(prefix=file_name.split(NC_EXTENSION)[0], 
                                        suffix=NC_EXTENSION,
                                        dir=self._temporary_folder)

        delayed:Any = dataset.to_netcdf(netcdf_file, compute=False)
        with ProgressBar():
            res: Any = delayed.compute()
        dataset.close()

        error_code = subprocess.call(["rm", output_file])
        if error_code != 0:
            raise OSError("Unable to delete the copied grib file. Error code: " + str(error_code))        

        os.close(file_descriptor)
        file_name_new: str = file_name.split(extension)[0] + NC_EXTENSION
        return (netcdf_file,file_name_new)

