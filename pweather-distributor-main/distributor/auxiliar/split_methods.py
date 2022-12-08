import logging
import subprocess
from typing import Any, Dict, Iterator, Optional, Union
from enum import Enum

import xarray
import numpy as np
import numpy.typing as npt

from aux_functions.aux_functions import create_file_name_from_month_year, open_dataset_with_file_name

class SplitType(Enum):
    """Type representing the different ways a file can be split
    according to it's time dimension
    """
    DAY = 1
    HOUR = 2

class SplitFileResult:
    """
    Represents a section of previously downloaded file that was split
    """

    def __init__(self, file_name: str, dataset: xarray.Dataset) -> None:
        self._file_name: str = file_name
        self._dataset: xarray.Dataset = dataset
        self._final_file_path: Optional[str] = None

    @property
    def file_name(self) -> str:
        """Returns the final name of the file

        Returns:
            str: file name
        """
        return self._file_name

    def store_file(self, file_path: str) -> None:
        """Stores the file in a netcdf format

        Args:
            file_path (str): path where file should be stored
        """
        self._dataset.to_netcdf(file_path)
        self._final_file_path = file_path
    
    def delete_stored_file(self) -> None:
        """
        Deletes the created file
        """
        if not self._final_file_path is None:
            subprocess.call(["rm", self._final_file_path])



def split_file(file_name: str, split_type: SplitType, initial_time_dim:str, 
    variation_time_dim:str,number_of_nodes:int) -> Iterator[SplitFileResult]:
    """
    Opens an xarray compatible file and splits it accross its time dimension

    Args:
        file_name (str): downloaded file
        split_type (SplitType): how the file should be split
        NOTE: the split_type argument was created, because it was originally thought 
        that it would be necessary to distribute sometimes by hours, other times by days,
        etc. This was not the case, and it is only possible to distribute by hours.

        initial_time_dim (str): dimension that represents the first timestamp
        variation_time_dim (str): dimension that represents the variation time dimension
        number_of_nodes (int): number of existing nodes

    Yields:
        Iterator[SplitFileResult]: resulting partition from file split
    """
    dataset: xarray.Dataset = open_dataset_with_file_name(file_name)

    time_date: Union[np.datetime64, npt.NDArray[np.datetime64]] #only a Union since can return both kinds
                                                                #with .values attribute (even tho only the
                                                                #np.datetime64 is relevant for this context)
    step_values: npt.NDArray[np.timedelta64]

    time_date = dataset.coords[initial_time_dim].values
    step_values = dataset.coords[variation_time_dim].values

    if not isinstance(time_date, np.datetime64):
        time_date = time_date + step_values[0]

    dataset_section: xarray.Dataset
    new_file_name: str

    for node in range(number_of_nodes):
        if split_type == SplitType.DAY:
            raise NotImplementedError("Not implemented")
        elif split_type == SplitType.HOUR:
            params: Dict[str, Any] = {}
            params[variation_time_dim] = \
                list(map(lambda pair: pair[1], 
                    filter(lambda pair: pair[0]%number_of_nodes == node, enumerate(step_values))))
            dataset_section = dataset.sel(params)
            new_file_name = create_file_name_from_month_year(time_date)
        
        yield SplitFileResult(new_file_name,dataset_section)

    dataset.close()
