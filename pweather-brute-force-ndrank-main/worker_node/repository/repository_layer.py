import numpy as np
import xarray
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, Type
from repository.auxiliary_structures.dataset_indexer import DateContainer
from repository.auxiliary_structures.constants import DATA_VARS, STEP, TIME_GAP, TIME_INITIAL_DIM, TIME_VARIATION_DIM
from repository.auxiliary_structures.time_gap_container import TimeGapContainer
from schema import Schema #type: ignore

class RepositoryMetadata:
    """
    Class that manages the metadata obtained from the
    settings file from the dataset
    """

    _time_gap_schema: Schema = Schema(
        {
            str: {
                int: [str]
            }
        }
    )

    def __init__(self, info: Dict[str, Any]) -> None:
        self._step: int = info[STEP]
        self._time_variation_dim: str = info[TIME_VARIATION_DIM]
        self._time_initial_dim: str = info[TIME_INITIAL_DIM]
        self._time_gap_container: TimeGapContainer = TimeGapContainer()
        self._data_vars: Set[str] = set(info[DATA_VARS])
        if TIME_GAP in info:
            RepositoryMetadata._time_gap_schema.validate(info[TIME_GAP])
            for time_tag in info[TIME_GAP]:
                for time_value in info[TIME_GAP][time_tag]:
                    for data_var in info[TIME_GAP][time_tag][time_value]:
                        self._time_gap_container.add_time_gap(time_tag,time_value,data_var)
    
    @property
    def step(self) -> int:
        """Value of the time difence in the time dimension

        Returns:
            int: integer value
        """
        return self._step

    @property
    def time_initial_dim(self) -> str:
        """Dimensions that represents the starting date of 
        time dimension in the given file

        Returns:
            str: name of the dimension
        """
        return self._time_initial_dim

    @property
    def time_variation_dim(self) -> str:
        """Dimension will all existing timestamps in the 
        dataset

        Returns:
            str: name of the dimension
        """
        return self._time_variation_dim

    @property
    def time_gap_container(self) -> TimeGapContainer:
        """Container that verifies if the dataset has
        gaps in its data in its time dimension.

        Returns:
            TimeGapContainer: Object capable of validating if 
            the timestamp is part of a gap in the data or not
        """
        return self._time_gap_container

    @property
    def data_vars(self) -> Set[str]:
        """Data variables available in the repository

        Returns:
            List[str]: existing data variables
        """
        return self._data_vars

class RepositoryLayer:
    """
    Class for the representation of the repository layer

    For the context of this system, the repository layer
    acts as a façade for the existing dataset.

    This class defines the main structure any implementation
    of the repository layer should have
    """
    def __init__(self, dataset_path: str, index_file_name: str) -> None:
        if len(dataset_path) == 0 or len(index_file_name) == 0:
            raise ValueError("dataset_path or index_file_name can not be empty")
        self._dataset_path: str = \
            dataset_path + "/" if dataset_path[-1] != "/" else dataset_path
        self._index_file: str = index_file_name

    def get_metadata(self) -> RepositoryMetadata:
        """Returns the existing attributes from the settings file

        Returns:
            RepositoryMetadata: received metadata from settings file
        """
        raise NotImplementedError("Method must be overriden")


    def get_low_resolution_parameters(self) -> Dict[str, int]:
        """Reads the metadata and returns the parameters that 
        reffer on how the resolution of the dataset was reduced

        Returns:
            Dict[str, int]: dimension together with their resolution factor
        """
        raise NotImplementedError("Method must be implemented")

    def get_dataset(self) -> Iterator[Tuple[str,xarray.Dataset]]:
        """
        Method used to iterate the full dataset

        Raises:
            NotImplementedError: supposed to be overriden
        """
        raise NotImplementedError("Method must be overriden")

    def get_dataset_part(self, date_container: DateContainer) -> Optional[Tuple[str, xarray.Dataset]]:
        """
        Returns the files that possess the region pointed by the heuristic result

        Args:
            date_container (DateContainer): date where the result should be searched

        Returns:
            Optional[Tuple[str, xarray.Dataset]]: File matching the heuristic 
            result or None, if the file is not found -> (file path, xarray object)

        Raises:
            NotImplementedError: supposed to be overriden
        """
        raise NotImplementedError("Method must be overriden")

    @property
    def dividing_unit(self) -> DateContainer:
        """
        Returns the time unit by which the existing files are split
        Returns:
            DateContainer: _description_
        """
        raise NotImplementedError("Method must be overriden")

    def clean_auxiliary_files(self) -> None:
        """
        Clear any created file like .idx files

        Raises:
            NotImplementedError: supposed to be overriden
        """
        raise NotImplementedError("Method must be overriden")

    def close_dataset_file(self, dataset: xarray.Dataset) -> None:
        """Function used to close dataset files

        Args:
            dataset (xarray.Dataset): dataset to be closed
        """
        dataset.close()

class DummyRepositoryMetadata(RepositoryMetadata):
    """Used only for test unit purposes

    """
    def __init__(self, info: Dict[str, Any]) -> None:
        pass
