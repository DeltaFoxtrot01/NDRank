import logging
from typing import Dict, Final, Iterator, Optional, Tuple, Union
from xmlrpc.client import Boolean
import numpy as np
import numpy.typing as npt
import  xarray, yaml
from auxiliar.xarray_aux import open_dataset_with_file_name
from repository.auxiliary_structures.dataset_indexer import HOUR_DAY_MONTH_YEAR_DATASET, MONTH_YEAR_DATASET, PROCESSING_FUNCTIONS, DatasetIndexer, DateContainer, subtract_timedelta
from repository.auxiliary_structures.constants import HOUR_DAY_MONTH_YEAR_REPO, MONTH_YEAR_REPO, MONTH_YEAR_ROUND_ROBIN_REPO
from repository.repository_layer import RepositoryLayer, RepositoryMetadata
from auxiliar.component_injector import component_injector

METADATA: Final = "metadata"
RESOLUTION_REDUCTION_PARAMETERS: Final = "resolution-reduction-parameters"

class _standardERA5FileFormat(RepositoryLayer):
    """
    Repository that allows the execution of the brute force search method.

    It iterates the existing files ordered by time
    """

    def __init__(self, dataset_path: str, index_file_name: str, function_name: str) -> None:
        """Sets up the basic parameters and creates the required structure
        to index the existing dataset.

        Args:
            dataset_path (str): folder path of the dataset
            index_file_name (str): name of the file with existing files
            function_name (str): function that should be used for the file convertion into the DateContainer
        Raises:
            ValueError: if dataset_path or index_file_name are provided as empty strings
        """
        super().__init__(dataset_path, index_file_name)
        self._dataset_index: DatasetIndexer = \
            DatasetIndexer(self._dataset_path + self._index_file,
                           self._dataset_path,
                           PROCESSING_FUNCTIONS[function_name])

    def get_metadata(self) -> RepositoryMetadata:
        """Returns info found in metadata block of index file

        Returns:
            Dict[str, Any]: contents in dictionary format
        """
        with open(self._dataset_path + self._index_file, "r") as stream:
            return RepositoryMetadata(yaml.safe_load(stream)[METADATA])

    def get_low_resolution_parameters(self) -> Dict[str, int]:
        """Reads the metadata and returns the parameters that reffer on how the resolution of the dataset was reduced

        Returns:
            Dict[str, int]: dimension together with their resolution factor
        """
        with open(self._dataset_path + self._index_file, "r") as stream:
            try:
                return yaml.safe_load(stream)[METADATA][RESOLUTION_REDUCTION_PARAMETERS]
            except KeyError:
                raise ValueError("Key " + RESOLUTION_REDUCTION_PARAMETERS + " not found in file " + self._dataset_path + self._index_file)

    def get_dataset(self) -> Iterator[Tuple[str, xarray.Dataset]]:
        """
        Returns every file ordered by time

        Yields:
            Iterator[Tuple[str, xarray.Dataset]]: returns every existing 
            file one by one
        """
        for path in self._dataset_index.get_sorted_file_paths():
            yield (path[1], open_dataset_with_file_name(path[1]))

    def get_dataset_part(self, date_container: DateContainer) -> Optional[Tuple[str, xarray.Dataset]]:
        """
        Returns the file 

        Args:
            date_container (DateContainer): date that is necessary

        Returns:
            Optional[Tuple[str, xarray.Dataset]]: file with the containing date
        """
        result: Optional[Tuple[int,str,DateContainer]] = \
            self._dataset_index.index_dataset(date_container)
        if result is None:
            return None
        else:
            return (result[1], open_dataset_with_file_name(result[1]))


@component_injector.inject_repository(MONTH_YEAR_REPO)
class MonthYearRepository(_standardERA5FileFormat):
    """Repository that allows the access to data that is split in files,
    each having the data for a full month
    """
    def _calculate_shift(self) -> None:
        file: Tuple[int, str, DateContainer] = self._dataset_index.get_random_file()
        ds: xarray.Dataset = open_dataset_with_file_name(file[1])
        metadata: RepositoryMetadata = self.get_metadata()
        time: np.datetime64 = ds.coords[metadata.time_initial_dim].values + \
                              ds.coords[metadata.time_variation_dim].values[0]
        self._file_shift = time - file[2].to_datetime64()

    def __init__(self, dataset_path: str, index_file_name: str) -> None:
        super().__init__(dataset_path, index_file_name, MONTH_YEAR_DATASET)
        self._calculate_shift()

    def get_dataset_part(self, date_container: DateContainer) -> Optional[Tuple[str, xarray.Dataset]]:
        if date_container.has_day() and date_container.has_hour():
            date_container = subtract_timedelta(date_container,self._file_shift)
        date_container.unset_day()
        date_container.unset_hour()
        return super().get_dataset_part(date_container)


@component_injector.inject_repository(HOUR_DAY_MONTH_YEAR_REPO)
class HourDayMonthYearRepository(_standardERA5FileFormat):
    """Repository that manages datasets where the files
    are divide by hours. 

    NOTE: this repository has not been used, as its usecase made the
    system very inefficient. It may be not fully functional 
    """
    def __init__(self, dataset_path: str, index_file_name: str) -> None:
        super().__init__(dataset_path, index_file_name, HOUR_DAY_MONTH_YEAR_DATASET)

@component_injector.inject_repository(MONTH_YEAR_ROUND_ROBIN_REPO)
class MonthYearRoundRobinRepository(MonthYearRepository):
    """Repository that deals with data that was distributed with a round robin strategy.
    It assumes that the files are organized in months.

    Does not allow a sequential iteration
    """
    def get_dataset(self) -> Iterator[Tuple[str, xarray.Dataset]]:
        raise NotImplementedError("This repository does not support sequential iteration")

    def _verify_if_step_exists(self,ds: xarray.Dataset, dt64: np.datetime64) -> Boolean:
        metadata: RepositoryMetadata = self.get_metadata()
        time_date: Union[np.datetime64, npt.NDArray[np.datetime64]] #only a Union since can return both kinds
                                                                    #with .values attribute (even tho only the
                                                                    #np.datetime64 is relevant for this context)
        step_values: npt.NDArray[np.timedelta64]
        step_values = ds.coords[metadata.time_variation_dim].values
        time_date = ds.coords[metadata.time_initial_dim].values
        return dt64 - time_date in step_values

    def get_dataset_part(self, date_container: DateContainer) -> Optional[Tuple[str, xarray.Dataset]]:
        """Returns the right file storing file pointers, validating if the file exists there

        Args:
            date_container (DateContainer): date used for search

        Returns:
            dataset (Optional[Tuple[str, xarray.Dataset]]): dataset with the containing timestamp
        """
        dt64: np.datetime64 = date_container.to_datetime64()
        res: Optional[Tuple[str, xarray.Dataset]]
        res = super().get_dataset_part(date_container)
        if res is None or not self._verify_if_step_exists(res[1],dt64):
            return None
        else:
            return res
