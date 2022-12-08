from typing import Iterator, Tuple

import xarray
from auxiliar.component_injector import component_injector
from repository.repository_layer import RepositoryLayer
from repository.auxiliary_structures.constants import DEV_DUMMY_TAG

@component_injector.inject_repository(DEV_DUMMY_TAG)
class DummyRepository(RepositoryLayer):
    """
    Dummy repository used for testing other components
    """
    def __init__(self, dataset_path: str, index_file_name: str) -> None:
        super().__init__(dataset_path, index_file_name)

    def get_dataset(self) -> Iterator[Tuple[str,xarray.Dataset]]:
        pass
