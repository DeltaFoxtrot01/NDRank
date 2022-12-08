import subprocess
import os
import xarray
from typing import Final, List, Tuple
import tempfile
from aux_functions.xarray_aux import open_dataset_with_file_name

NC: Final = ".nc"

def split_file_by_data_variables(source_file: str, file_name: str, temporary_folder: str) -> List[Tuple[str,str,str]]:
    """Used to split files by data variables.
    Receives a file in the grib or netcdf format, evaluates every existing data var
    and split in multiple netcdf files by data variable.

    Created to allow the split of datasets into multiple dataset, each with a single data var 

    Args:
        source_file (str): path of the file to be split 
        file_name (str): original name of the file
        temporary_folder (str): folder used to create temporary files

    Returns:
        List[Tuple[str,str,str]]: list of files organized in tuples with the following parameters:
            (path of the created file, intended name of the file, data variable containing it)
    """
    res: List[Tuple[str,str,str]] = []
    ds: xarray.Dataset = open_dataset_with_file_name(source_file)
    extension: str = "." + file_name.split(".")[-1]
    final_file_name: str = file_name.split(extension)[0] + NC

    for data_var in ds.data_vars.keys():
        file_descriptor, output_file = tempfile.mkstemp(prefix=file_name.split(extension)[0], 
                                                suffix=NC, 
                                                dir=temporary_folder)
        os.close(file_descriptor)
        ds[[data_var]].to_netcdf(output_file)
        res.append((output_file, final_file_name, data_var))
    return res


def clean_aux_split_files(temporary_folder: str) -> None:
    if temporary_folder[-1] != "/":
        temporary_folder += "/"

    entries: List[str] = os.listdir(temporary_folder)
    for file in entries:
        if file == ".gitignore":
            continue
        error_code:int = subprocess.call(["rm", "-r",temporary_folder + file])
        if error_code != 0:
            raise FileNotFoundError("Cached file not found. Error Code: " + str(error_code))
        



