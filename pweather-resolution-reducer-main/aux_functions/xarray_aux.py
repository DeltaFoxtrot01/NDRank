from typing import Dict, Optional
import xarray


def open_dataset_with_file_name(file: str, chunked: Optional[Dict] = None) -> xarray.Dataset:
    """
    Opens a dataset file correctly according to the extension
    Args:
        file (str): file to be opened
        chunked (Optional[Dict]): if file should be opened with dask arrays

    Returns:
        xarray.Dataset: resulting dataset

    Raises:
        ValueError: if the extension is unknown    
    """
    if chunked is None:
        if file.endswith(".nc"):
            return xarray.open_dataset(file)
        elif file.endswith(".grib"):
            return xarray.open_dataset(file, engine="cfgrib")
        else:
            raise ValueError("Dataset file does not end with a valid extension")
    else:
        if file.endswith(".nc"):
            return xarray.open_dataset(file, chunks=chunked)
        elif file.endswith(".grib"):
            return xarray.open_dataset(file, engine="cfgrib", chunks=chunked)
        else:
            raise ValueError("Dataset file does not end with a valid extension")
        

def open_dataarray_with_file_name(file: str) -> xarray.DataArray:
    """
    Opens a dataarray file correctly according to the extension
    Args:
        file (str): file to be opened

    Returns:
        xarray.DataArray: resulting dataarray

    Raises:
        ValueError: if the extension is unknown    
    """
    if file.endswith(".nc"):
        return xarray.open_dataarray(file)
    elif file.endswith(".grib"):
        return xarray.open_dataarray(file, engine="cfgrib")
    else:
        raise ValueError("Dataarray file does not end with a valid extension")