import os
import tempfile
from typing import Optional, Tuple
import xarray as xray
import cdo #type: ignore
from xarray.core.dataset import Dataset
from resolution_reducer_tools.resolution_reducer import resolution_reducer_interface


class grib_sp_to_gp_reducer(resolution_reducer_interface):
    """Converts the grid from the spherical harmonic to a reduced gaussian grid
    """
    def reduce_resolution(self, source_file: str, file_name: str, previous_file: Optional[str] = None, 
        next_file: Optional[str] = None) -> Tuple[str, str]:
        output_file: str
        file_descriptor: int
        cdo_tool: cdo.Cdo = cdo.Cdo()
        file_descriptor, output_file = tempfile.mkstemp(prefix=file_name.split(".grib")[0], 
                                            suffix=".grib", 
                                            dir=self._temporary_folder)
        dataset: Dataset = xray.open_dataset(source_file, engine="cfgrib")

        if "latitude" in dataset.coords or "longitude" in dataset.coords:
            dataset.close()
            raise ValueError("Dataset already is a grid")
        dataset.close()

        cdo_tool.sp2gp(input=source_file, 
                       output=output_file)

        os.close(file_descriptor)
        self._add_temporary_file(output_file)
        return (output_file, file_name)

    #def clear_cached_file(self, file_path: str):
    #    error_code:int = subprocess.call(["rm", file_path])
    #    if error_code != 0:
    #        raise FileNotFoundError("Cached file not found. Error Code: " + str(error_code))
