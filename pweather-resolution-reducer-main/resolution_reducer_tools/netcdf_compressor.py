import logging
import subprocess
from typing import Optional, Tuple
import xarray
from aux_functions.xarray_aux import open_dataset_with_file_name
from resolution_reducer_tools.resolution_reducer import resolution_reducer_interface


class NetcdfCompressor(resolution_reducer_interface):
    """Reduces the size in bytes of netcdf files meanwhile keeping the 
    contents "the same"
    """
    def __init__(self, temporary_folder: str):
        super().__init__(temporary_folder)
        self._temp_folder_nccompress: str = self._temporary_folder + "tmp/"
        subprocess.call(["mkdir",self._temp_folder_nccompress])

    def reduce_resolution(self, source_file: str, file_name: str, previous_file: Optional[str] = None,
        next_file: Optional[str] = None) -> Tuple[str, str]:
        if not source_file.endswith(".nc"):
            raise ValueError("File is not in netcdf format")
        output_file: str = self._temp_folder_nccompress + file_name
        logging.debug(self._temp_folder_nccompress)
        subprocess.call(["nccompress", source_file, "-t", self._temp_folder_nccompress])

        ds1: xarray.Dataset = open_dataset_with_file_name(source_file,{'step':1,'time':1})
        ds2: xarray.Dataset = open_dataset_with_file_name(output_file,{'step':1,'time':1})

        if not xarray.Dataset.equals(ds1,ds2):
            raise ValueError("Compressed version of file " + file_name + " do not match")

        ds1.close()
        ds2.close()

        self._add_temporary_file(output_file)
        return (output_file, file_name)
