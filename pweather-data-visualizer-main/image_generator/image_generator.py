from datetime import datetime, timedelta
from matplotlib.axes import Axes #type: ignore
import matplotlib.pyplot as plt #type: ignore
import logging
from typing import Dict, Final, Tuple, Union
import numpy as np
import xarray, yaml
from auxiliar.auxiliar import open_dataset_with_file_name, open_dataarray_with_file_name
from properties_processor.processor import SettingsResults

import cartopy #type: ignore
import cartopy.crs as ccrs #type: ignore

AVERAGE_FILE_NAME: Final = "average.yaml"
STANDARD_DEVIATION_FILE_NAME: Final = "standard_deviation.yaml"

plt.set_loglevel("info")

class ParameterFileCollection:
    """Manages the parameter properties files
    and facilitates indexing
    """

    def __init__(self, path: str, file_name: str) -> None:
        """Basic contructor. Opens the file and stores the required
        structures to index these files

        Args:
            path (str): path where all the files can be found
            file_name (str): name of the yaml file that indexes existing files
        """
        self._files: Dict[int,Dict[int,Dict[int,str]]] #the keys go in the following order: month,day,hour
        self._file_path: str = path
        with open(self._file_path + file_name, 'r') as f:
            self._files = yaml.safe_load(f)

    def get_array(self, month: int, day: int, hour: int) -> xarray.DataArray:
        """Indexes file that matches the given day

        Args:
            month (int): month required
            day (int): day required
            hour (int): hour required

        Returns:
            str: path of the file
        """
        return open_dataarray_with_file_name(self._file_path + self._files[month][day][hour])


class ImageGenerator:

    def _convert_to_step(self, ds: xarray.Dataset, 
        date: datetime) -> Union[np.datetime64,np.timedelta64]:

        date64: np.datetime64 = np.datetime64(date)
        res: Union[np.datetime64, np.timedelta64] = \
            date64 - ds.coords[self._props.time_init_dim].values #type: ignore
        return res

    def _select_params_from_dataset(self, date: datetime) -> xarray.Dataset:
            ds: xarray.Dataset = open_dataset_with_file_name(
                self._files[(date.month,date.year)]
            )
            ts: Union[np.datetime64,np.timedelta64] = \
                self._convert_to_step(ds,date)
            if not ts in ds.coords[self._props.time_var_dim]:
                ds.close()
                date = date - timedelta(days=1)
                ds = open_dataset_with_file_name(
                    self._files[(date.month,date.year)]
                )
                ts = self._convert_to_step(ds,date)
            params: Dict = {}
            params[self._props.time_var_dim] = ts
            params = params | self._props.other_params 
            return ds.sel(params)

    def __init__(self, props: SettingsResults, 
        files: Dict[Tuple[int,int],str]) -> None:
        self._props: SettingsResults = props
        self._files: Dict[Tuple[int,int],str] = files
        self._res_folder: str = self._props.result_folder_path
        self._average_params: ParameterFileCollection
        self._standard_params: ParameterFileCollection

        if self._res_folder[-1] != "/":
            self._res_folder += "/"

        if self._props.subtract_average:
            average_path: str = self._props.average_path
            if average_path[-1] != "/":
                average_path += "/"
            self._average_params = \
                ParameterFileCollection(average_path, AVERAGE_FILE_NAME)
        if self._props.divide_standard_deviation:
            standard_deviation: str = self._props.standard_deviation_path
            if standard_deviation[-1] != "/":
                standard_deviation += "/"
            self._standard_params = \
                ParameterFileCollection(standard_deviation, STANDARD_DEVIATION_FILE_NAME)

    def generate(self) -> None:
        for date in self._props.wanted_dates:
            ds: xarray.Dataset = self._select_params_from_dataset(date)

            for var in self._props.data_vars:
                fig = plt.figure(figsize=(10, 7))
                plt.title("ERA5 - " + str(date) + " - " + str(var))
                ax: Axes = plt.axes(projection=ccrs.Robinson())
                ax.coastlines(resolution="10m")

                da: xarray.DataArray = ds.sel()[var]
                
                if self._props.subtract_average:
                    da = da - self._average_params.get_array(date.month,date.day,date.hour).sel(variable=var)

                if self._props.divide_standard_deviation:
                    std_da: xarray.DataArray = self._standard_params\
                                                        .get_array(date.month,date.day,date.hour).sel(variable=var)
                    std_da = std_da.where(std_da.values != 0).fillna(10**(-10))
                    da = da / std_da
                
                da.plot(
                    cmap=plt.cm.coolwarm, transform=ccrs.PlateCarree(), cbar_kwargs={"shrink": 0.6}
                )
                
                plt.draw()
                file_name: str = self._res_folder + str(date) + \
                                    "-" + var + '.png'
                logging.info("Generating file " + file_name)
                plt.savefig(file_name, dpi=1000)
                plt.close()
            ds.close()

