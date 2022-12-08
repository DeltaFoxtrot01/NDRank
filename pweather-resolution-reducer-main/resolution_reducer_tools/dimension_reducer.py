import logging
import subprocess
import os
import tempfile
from typing import Dict, Final, List, Optional, Tuple
import numpy as np

from xarray import Dataset
import xarray
from aux_functions.xarray_aux import open_dataset_with_file_name
from resolution_reducer_tools.resolution_reducer import resolution_reducer_interface

GRIB: Final = ".grib"
NC: Final = ".nc"

FINAL_EXTENSION: Final = ".nc"

class dimension_reducer(resolution_reducer_interface):
    """
    Basic reducer for any given kind of dimensions. Works with both grib and netCDF files.
    """

    def __init__(self, temporary_folder: str, dimension_settings: Dict[str,int]):
        """Constructor of the reducer. The "dimension_settings" works in the following format:
        A dictionary is given where its key is the name of the dimension and its value is the
        reduction factor for that dimension, like this:
        {
            "latitude": 3
            "longitude": 4
        }

        For this example, the latitude dimension will be reduced by a factor of 3 and the 
        longitude dimension will be reduced by a factor of 4

        Args:
            temporary_folder (str): path of the temporary file
            dimension_settings (Dict[str,int]): dictionary with the given dimensions
        """
        super().__init__(temporary_folder)
        self._dimension_settings: Dict[str, int] = dimension_settings


    def reduce_resolution(self, source_file: str, file_name: str, previous_file: Optional[str] = None, 
        next_file: Optional[str] = None) -> Tuple[str, str]:
        """Executes the reduce operation

        Args:
            source_file (str): source of the file
            file_name (str): name of the file

        Raises:
            ValueError: If any given value is invalid
            OSError: In case of an error with any executed OS command

        Returns:
            Tuple[str, str]: path of the created file and name of the newly created file
        """
        """
        The resolution process of resolution process evolves the following steps
        1) verify if the file in the right format
        2) create a local copy
        3) reduce the dataset resolution and store it in a netcdf format
        5) delete the old copy

        the dataset is stored in netcdf, because it is what xarray provides.
        """
        file_descriptor: int
        output_file: str
        file_descriptors: List[int] = []
        reduced_file_output: str

        #step 1) verify if the file in the right format
        extension: str
        if GRIB in file_name:
            extension = GRIB
        elif NC in file_name:
            extension = NC
        else:
            raise ValueError("file has an unknown extension: " + source_file)

        file_descriptor, output_file = tempfile.mkstemp(prefix=file_name.split(extension)[0], 
                                                        suffix=extension,
                                                        dir=self._temporary_folder)

        file_descriptors.append(file_descriptor)
        dataset: Dataset = open_dataset_with_file_name(source_file)
        
        for dim in self._dimension_settings:
            if not dim in dataset.dims:
                dataset.close()
                raise ValueError("Dimension " + dim + " is not available in file")
        dataset.close()

        #step 2) create a local copy
        error_code:int = subprocess.call(["cp",source_file,output_file])
        if error_code != 0:
            raise OSError("Unable to create copy of file " + source_file + ". Error code: " + str(error_code))

        #step 3) reduce the dataset resolution and store the final file
        dataset = open_dataset_with_file_name(output_file)
        dataset_aux: xarray.Dataset
        for coord in self._dimension_settings:
            dataset_aux = dataset.coarsen({
                                coord: self._dimension_settings[coord]
                             },boundary="trim")\
                             .mean()

            """This piece of code is here is here as it is useful for the step and time dimension keep old 
            dimension values instead of the average "between the selected regions to reduce".
            This way, the low resolution dataset will return results that exist in the full resolution dataset
            and then the missing timestamps can be covered by the neighbour gap parameter.
            """
            if coord in ("step", "time"):
                final_size: int = len(dataset[coord]) - len(dataset[coord]) % self._dimension_settings[coord]
                dataset_aux = dataset_aux.assign_coords({
                    coord: np.array(
                        [dataset[coord][i].values for i in range(0,final_size,self._dimension_settings[coord])])
                })

            dataset = dataset_aux

        file_descriptor, reduced_file_output = \
                    tempfile.mkstemp(prefix=file_name.split(extension)[0], 
                                     suffix=FINAL_EXTENSION,
                                     dir=self._temporary_folder)
        file_descriptors.append(file_descriptor)
        dataset.to_netcdf(reduced_file_output)
        dataset.close()
        
        #step 4) delete the old copy
        error_code = subprocess.call(["rm", output_file])
        if error_code != 0:
            raise OSError("Unable to delete the copied file. Error code: " + str(error_code))
        
        for file_descriptor in file_descriptors:
            os.close(file_descriptor)

        #step 5) return the final result
        return (reduced_file_output, file_name.split(extension)[0] + FINAL_EXTENSION)