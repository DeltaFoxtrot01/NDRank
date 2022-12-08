from argparse import ArgumentError
import logging
from typing import Any, Callable, Dict, List, Optional
import datetime as dt
from typing_extensions import Final
import numpy as np

import numpy.typing as npt
import xarray

from aux_functions.aux_functions import open_dataset_with_file_name

TIME_INTERVAL: Final = "time-interval"


MAIN_TIME_DIMENSION: Final = 'time'
USED_TIME_DIMENSION: Final = 'step'

class strategy_interface:
    """
    Interface responsible for defining how a distribution strategy should work
    """

    def __init__(self, key_strategy: str) -> None:
        """
        Basic constructor to save information helpfull for sorting

        Accepted name formats:
            - mock -> <integer number>
            - reduced -> '%Y-%m-%d %H:%M:%S'
            - grib -> ERA5-<month>-<year>.<grib or nc>
        Args:
            key_strategy (str): name of the chosen strategy
        """
        self._key_strategy_collection: Dict[str, Callable] = {
            "reduced": self._key_reduced,
            "mock": self._key_mock,
            "grib": self._key_grib
        }
        if not key_strategy in self._key_strategy_collection:
            raise ValueError("Key strategy " + key_strategy + " does not exist")
        self._chosen_strategy: str = key_strategy


    def _key_reduced(self, time:str) -> float:
        try:
            return dt.datetime.strptime(time, '%Y-%m-%d %H:%M:%S').timestamp()
        except ValueError:
            raise ValueError("File presents invalid name format:\t" + time)
            
    def _key_mock(self, time:str) -> float:
        try:
            return int(time.split("ERA5-1-")[-1].split(".")[0])
        except ValueError:
            raise ValueError("String is not an int:\t" + time)

    def _key_grib(self, time:str) -> float:
        try:
            aux: List[str] = time.split("ERA5-")[1].split(".grib")[0].split(".nc")[0].split("-")
            return int(aux[1])*12 + int(aux[0])
        except Exception:
            raise ValueError("File presents an invalid format: " + time)

    def _get_strategy(self) -> Callable:
        return self._key_strategy_collection[self._chosen_strategy]

    def split(self, object_names: List[str], number_nodes: int, starting_node: Optional[int] = None) -> List[List[str]]:
        """
        Method responsible for defining the distribution of the objects

        Args:
            object_names (List[str]): List of objects in the source
            number_nodes (int): Number of nodes
            starting_node (Optional[int]): which node should be considered the first

        Returns:
            List[List[str]]: Lists of objects distributed by the number of nodes
        """
        pass


class metadata_strategy:
    """
    Interface to allow the development of different strategies to produce metadata
    for settings.yaml
    """

    def __init__(self, strategy_args: Optional[Dict[str, Any]] = None) -> None:
        """Basic constructor that holds information that may be essencial to the 
        specific strategy

        Args:
            strategy_args (Optional[Dict[str, Any]], optional): Attributes that may be useful. Defaults to None.
        """
        self._strategy_args: Optional[Dict[str, Any]] = strategy_args


    def get_metadata(self, file_path:str) -> Dict[str,Any]:
        """Method to return the given metadata of the file

        Args:
            file_path (str): path of the file

        Returns:
            Dict[str,Any]: metadata to be stored
        """
        pass


######################## IMPLEMENTATION TO BE USED ########################

class round_robin_strategy(strategy_interface):
    """
    Organizes the received files with the round robin strategy
    """
    def split(self, object_names: List[str], number_nodes: int) -> List[List[str]]:
        """
        Method responsible for defining the distribution of the objects

        Args:
            object_names (List[str]): List of objects in the source
            number_nodes (int): Number of nodes

        Returns:
            List[List[str]]: Lists of objects distributed by the number of nodes
        """
        if number_nodes <= 0:
            raise ValueError("number_nodes must be a positive number")

        object_names.sort(key=self._get_strategy())

        res: List[List[str]] = []
        for _ in range(number_nodes):
            res.append([])

        element_pos: int = 0
        node_pos: int = 0
        
        while element_pos < len(object_names):
            res[node_pos].append(object_names[element_pos])

            element_pos += 1
            if node_pos >= number_nodes - 1:
                node_pos = 0
            else:
                node_pos += 1

        return res


class time_interval_strategy(strategy_interface):
    """
    Organizes the receives files with the time interval strategy
    """

    def split(self, object_names: List[str], number_nodes: int) -> List[List[str]]:
        """
        Method responsible for defining the distribution of the objects

        Args:
            object_names (List[str]): List of objects in the source
            number_nodes (int): Number of nodes

        Returns:
            List[List[str]]: Lists of objects distributed by the number of nodes
        """
        res: List[List[str]] = []
        size: int = len(object_names)
        slice_size: int = size//number_nodes

        object_names.sort(key=self._get_strategy())
        
        left_border: int = 0
        right_border: int = slice_size
        borders: List[List[int]] = []

        """
        The next step defines which group of elements each node should have

            The first loop defines this groups with integer division, 
        this results in certain missing elements

            The second and third loop distributes the remaining objects using
        the remainder

            The final loop splits the elements accordingly
        """

        for _ in range(number_nodes):
            borders.append([left_border,right_border])
            left_border += slice_size
            right_border += slice_size
        
        if size%number_nodes != 0:
            border_pos: int = 0
            i: int = 0
            for i in range(size%number_nodes):
                borders[border_pos][0] += i
                borders[border_pos][1] += i + 1
                border_pos += 1

            for _ in range(number_nodes - size%number_nodes):
                borders[border_pos][0] += i + 1
                borders[border_pos][1] += i + 1
                border_pos += 1

        for border in borders:
            res.append(object_names[border[0]:border[1]])
        return res

TIME_VARIATION_DIM: Final = "time-variation-dim"
TIME_INITIAL_DIM: Final = "time-initial-dim"
REDUCTION_PARAMS: Final = "resolution-reduction-parameters"
STEP: Final = "step"
TIME_GAP: Final = "time-gap"
DATA_VARS: Final = "data-vars"

class grib_netcdf_metadata(metadata_strategy):
    """
    Reads the file and gathers metadata about the time dimension
    """
    def __init__(self, strategy_args: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(strategy_args)
        if self._strategy_args is None:
            raise ArgumentError("For grib_netcdf, it is necessary to pass the metadata attributes")
        self._time_var_dim: str = self._strategy_args[TIME_VARIATION_DIM]
        self._time_init_dim: str = self._strategy_args[TIME_INITIAL_DIM]
        self._time_gap: Optional[Dict[str, Any]] = None
        self._time_var: float = self._strategy_args[STEP] #time value of each step
        if TIME_GAP in self._strategy_args:
            self._time_gap = self._strategy_args[TIME_GAP]

    def get_metadata(self, file_path:str) -> Dict[str, Any]:
        res: Dict[str, Any] = {}

        dataset: xarray.Dataset = open_dataset_with_file_name(file_path)

        res[USED_TIME_DIMENSION] = self._time_var
        res[TIME_VARIATION_DIM] = self._time_var_dim
        res[TIME_INITIAL_DIM] = self._time_init_dim

        res[DATA_VARS] = list(map(lambda x: x, dataset.data_vars.keys()))
        
        if not self._time_gap is None:
            res[TIME_GAP] = self._time_gap
        
        dataset.close()
        return res

class low_res_grib_netcdf_metadata(grib_netcdf_metadata):
    """
    Reads the file and gathers metadata about the time dimension
    (just like the grib_netcdf_metadata strategy) and the attributes
    required to understand how the resolution was reduced
    """
    def __init__(self, strategy_args: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(strategy_args)
        self._vars_for_low_res: Dict[str, int] = self._strategy_args[REDUCTION_PARAMS]

    def get_metadata(self, file_path: str) -> Dict[str, Any]:
        res: Dict[str,Any] = super().get_metadata(file_path)
        res[REDUCTION_PARAMS] = self._vars_for_low_res
        return res

class dummy_metadata(metadata_strategy):

    def get_metadata(self, file_path: str) -> Dict[str, Any]:
        return {}