import logging
import os
import tempfile
import numpy as np
from typing import Any, Dict, Final, Optional, Tuple
import numpy.typing as npt

from xarray import Dataset
import xarray
from aux_functions.xarray_aux import open_dataset_with_file_name
from resolution_reducer_tools.resolution_reducer import resolution_reducer_interface

NC_EXTENSION: Final = ".nc"
GRIB_EXTENSION: Final = ".grib"

class CleanZeroSections(resolution_reducer_interface):
    """Removes sections in the time dimension where all values are zero
    """

    def __init__(self, temporary_folder: str, variation_dim: str, first_val_dim: str):
        super().__init__(temporary_folder)
        self._variation_dim: str = variation_dim
        self._first_val_dim: str = first_val_dim

    def reduce_resolution(self, source_file: str, file_name: str, previous_file: Optional[str] = None, 
        next_file: Optional[str] = None) -> Tuple[str, str]:
        file_descriptor: int
        netcdf_file: str

        dataset: Dataset = open_dataset_with_file_name(source_file)
        first_date: np.datetime64 = dataset.coords[self._first_val_dim].values #type: ignore
        step_values: npt.NDArray[np.timedelta64] = dataset.coords[self._variation_dim].values

        for step_index in range(len(step_values)):
            params: Dict[str,Any] = {}
            params[self._variation_dim] = step_values[step_index]
            ds_step:xarray.Dataset = dataset.sel(params)
            ds_step.apply(np.fabs)
            if not False in (ds_step.max().to_array().values == [0.0,0.0,0.0,0.0,0.0,0.0]):
                logging.debug("Removing " + str(step_values[step_index] + first_date))
                dataset = dataset.drop_sel(params)
        
        #creates the new file in a netcdf format
        logging.debug("creating copy and storing in netcdf format")
        extension: str
        if file_name.endswith(NC_EXTENSION):
            extension = NC_EXTENSION
        elif file_name.endswith(GRIB_EXTENSION):
            extension = GRIB_EXTENSION
        else:
            raise ValueError("File " + file_name + " does not have a known extension")
        file_descriptor, netcdf_file = tempfile.mkstemp(prefix=file_name.split(extension)[0], 
                                        suffix=NC_EXTENSION,
                                        dir=self._temporary_folder)
        os.close(file_descriptor)
        dataset.to_netcdf(netcdf_file)
        dataset.close()

        self._add_temporary_file(netcdf_file)
        return (netcdf_file, file_name.split(extension)[0] + extension)