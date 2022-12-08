import logging
import os
from statistics import mean
import subprocess
import tempfile
from typing import Final, List, Optional, Tuple
import xarray as xray
import numpy as np
from xarray.core.dataset import Dataset
from resolution_reducer_tools.resolution_reducer import resolution_reducer_interface


class grib_surface_reducer(resolution_reducer_interface):
    """
    resolution_reducer responsible for reducing the resolution of 
    grib files that present a grid with latitude and longitude for
    surface data.
    """

    def __init__(self, temporary_folder: str, factor: int):
        """
        Constructor for Grid Reducer

        Args:
            factor (int): required reduction factor
            temporary_folder (str): folder to create temporary files
        """
        super().__init__(temporary_folder)
        if factor <= 0:
            raise ValueError("Factor value must be a positive integer")
        self._factor: Final = factor


    def _reduce_surface_data(self, dataset: Dataset) -> Dataset:
        """
        Used to reduce the resolution of surface data

        Args:
            dataset (Dataset): dataset to be reduced
        """
        
        new_dataset: Dataset = dataset.coarsen(latitude=self._factor, boundary="trim")\
                                      .mean()\
                                      .coarsen(longitude=self._factor, boundary="trim")\
                                      .mean()
        return new_dataset

    def reduce_resolution(self, source_file: str, file_name: str, previous_file: Optional[str] = None, 
        next_file: Optional[str] = None) -> Tuple[str,str]:
        """
        Reduces the resolution of a surface kind of dataset

        Args:
            source_file (str): source of the file
            file_name (str): name of the file
        Returns:
            str: path of the created file
        """

        """
        The resolution process of resolution process evolves the following steps
        1) verify if the file in the right format
        2) create a local copy
        3) reduce the dataset resolution
        4) store it in a netcdf format
        5) delete the old copy
        6) return the name of the created file

        the dataset is stored in netcdf because it is what xarray provides.
        Support to store files in a GRIB format.
        """
        logging.warning("THIS REDUCER WILL BE DEPRECATED")
        file_descriptor: int
        grib_output_file: str
        file_descriptors: List[int] = []
        #step 1) verify if the file in the right format
        file_descriptor, grib_output_file = tempfile.mkstemp(prefix=file_name.split(".grib")[0], 
                                                        suffix=".grib",
                                                        dir=self._temporary_folder)

        file_descriptors.append(file_descriptor)
        netcdf_output_file: str
        netcdf_file_name: str
        dataset: Dataset = xray.open_dataset(source_file)#, engine="cfgrib")
        
        if not ("latitude" in dataset.dims and \
                "longitude" in dataset.dims):
            dataset.close()
            raise ValueError("Both Latitude and Longitude must be available")
        
        dataset.close()
        
        #step 2) create a local copy
        error_code:int = subprocess.call(["cp",source_file,grib_output_file])
        if error_code != 0:
            raise OSError("Unable to create copy of file " + source_file + ". Error code: " + str(error_code))
        
        #step 3) reduce the dataset resolution
        dataset = xray.open_dataset(grib_output_file) #, engine="cfgrib")
        print(dataset)
        if "surface" in dataset.coords:
            new_dataset: Dataset = self._reduce_surface_data(dataset)
        
        #step 4)
            file_descriptor, netcdf_output_file = tempfile.mkstemp(prefix=file_name.split(".grib")[0], 
                                                         suffix=".nc",
                                                         dir=self._temporary_folder)
            file_descriptors.append(file_descriptor)
            netcdf_file_name = file_name.split(".")[0] + ".nc"
            new_dataset.to_netcdf(netcdf_output_file)
            new_dataset.close()
        else:
            raise ValueError("Dataset does not represent surface data")

        # step 5) delete the old copy
        dataset.close()
        error_code = subprocess.call(["rm", grib_output_file])
        if error_code != 0:
            raise OSError("Unable to delete the copied grib file. Error code: " + str(error_code))
        
        for file_descriptor in file_descriptors:
            os.close(file_descriptor)

        self._add_temporary_file(netcdf_output_file)
        # step 6) return the name of the created file
        return (netcdf_output_file, netcdf_file_name)

    #def clear_cached_file(self, file_path: str):
    #    files: List[str] = [i for i in glob.glob(file_path + "*")]
    #    error_code:int = subprocess.call(["rm", "-r"] + files)
    #    if error_code != 0:
    #        raise FileNotFoundError("Cached file not found. Error code: " + str(error_code))

