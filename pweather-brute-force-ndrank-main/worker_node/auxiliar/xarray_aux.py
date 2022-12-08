import xarray


def open_dataset_with_file_name(file: str) -> xarray.Dataset:
    """
    Opens a dataset file correctly according to the extension
    Args:
        file (str): file to be opened

    Returns:
        xarray.Dataset: resulting dataset

    Raises:
        ValueError: if the extension is unknown    
    """
    if file.endswith(".nc"):
        return xarray.open_dataset(file)#, chunks={"latitude":15, "longitude": 30})
    elif file.endswith(".grib"):
        return xarray.open_dataset(file, engine="cfgrib")#, chunks={"latitude": 15, "longitude": 30})
    else:
        raise ValueError("Dataset file does not end with a valid extension")

def open_dataarray_with_file_name(file: str) -> xarray.DataArray:
    """
    Opens a data array file correctly according to the extension
    Args:
        file (str): file to be opened

    Returns:
        xarray.DataArray: resulting DataArray

    Raises:
        ValueError: if the extension is unknown    
    """
    if file.endswith(".nc"):
        return xarray.open_dataarray(file)#, chunks={"latitude":15, "longitude": 30})
    elif file.endswith(".grib"):
        return xarray.open_dataarray(file, engine="cfgrib")#, chunks={"latitude": 15, "longitude": 30})
    else:
        raise ValueError("DataArray file does not end with a valid extension")