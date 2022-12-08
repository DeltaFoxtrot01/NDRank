from calendar import month
from datetime import datetime
import logging
from typing import List
import pandas as pd
import numpy as np
import xarray

def open_dataset_with_file_name(file: str) -> xarray.Dataset:
    """
    Opens a dataset file correctly according to the extension.

    Implemented so the logic for grib files and netcdf files is
    not repeated all over the code.
    
    Args:
        file (str): file to be opened

    Returns:
        xarray.Dataset: resulting dataset

    Raises:
        ValueError: if the extension is unknown    
    """
    if file.endswith(".nc"):
        return xarray.open_dataset(file)
    elif file.endswith(".grib"):
        return xarray.open_dataset(file, engine="cfgrib")
    else:
        raise ValueError("Dataset file does not end with a valid extension")


def create_file_name(date: np.datetime64) -> str:
    """
    Returns a file name given a datetime64

    Args:
        date (np.datetime64): input date

    Returns:
        str: created file name
    """
    t = pd.Timestamp(date)
    return "ERA5-" + \
            str(t.hour)  + "-" + \
            str(t.day)   + "-" + \
            str(t.month) + "-" + \
            str(t.year)  + ".nc"

def create_file_name_from_month_year(date: np.datetime64) -> str:
    """
    Returns a file name with the month, year and node number

    Args:
        date (np.datetime64): input date
        node_num (int): node number

    Returns:
        str: created file name
    """
    t = pd.Timestamp(date)
    return "ERA5-" + \
            str(t.month) + "-" + \
            str(t.year)  + ".nc"



def key_function_for_file(file_name:str) -> datetime:
    """Converts a file name into a datetime

    Args:
        file_name (str): file name created by "create_file_name"

    Returns:
        datetime: datetime equivalent
    """
    sections: List[str] = file_name.split(".nc")[0].split("-")
    sections.pop(0)
    return datetime(int(sections[-1]),int(sections[-2]),1)