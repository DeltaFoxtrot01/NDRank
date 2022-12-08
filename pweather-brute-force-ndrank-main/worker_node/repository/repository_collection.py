from typing import Dict, List, Optional
import numpy as np
from repository.repository_layer import RepositoryLayer, RepositoryMetadata

"""This class was created in a latter stage of development. It's the class
that manages the existing repositories.
"""

class RepositoryCollection:
    """This class represents a single collection of repositories of the same type
    """

    def __init__(self, repositories: List[RepositoryLayer]) -> None:
        """Basic constructor for Repository Collection. Verifies if all 
        repositories are from the same type

        Args:
            repositories (List[RepositoryLayer]): list of repositories to build the collection
        """
        if len(repositories) == 0:
            raise ValueError("List of repositories for RepositoryManager is empty")

        first_elem: RepositoryLayer = repositories[0]

        for repo in repositories:
            if not isinstance(repo, type(first_elem)):
                raise ValueError("All repositories must be of the exact same type")
        
        self._repositories: List[RepositoryLayer] = repositories
    
    @property
    def repositories(self) -> List[RepositoryLayer]:
        """Returns the list of existing repositories

        Returns:
            List[RepositoryLayer]: list of existing repositories
        """
        return self._repositories
    
    @property
    def step_variation(self) -> int:
        """Returns the maximum existing step value of 
        all repositories, to work as a global step for
        all repositories

        Returns:
            int: highest step
        """
        return max(map(lambda x: x.get_metadata().step, self._repositories))

    def get_subsection_repositories(self, data_vars: List[str]) -> 'RepositoryCollection':
        """Receives a group of data variables and returns a subset of 
        repositories that have those variables. This exists in order to
        allow the service to interact only with the repositories that 
        have the data variables of the received request.

        Args:
            data_vars (List[str]): wanted data variables

        Returns:
            RepositoryCollection: resulting subset of repositories
            with the received data variables
        """
        final_list: List[RepositoryLayer] = []

        for repo in self._repositories:
            for var in data_vars:
                if var in repo.get_metadata().data_vars:
                    final_list.append(repo)
                    break
        
        return RepositoryCollection(final_list)

    def is_gap(self, ts: np.datetime64, data_vars: List[str], 
        search_hours: Optional[List[int]] = None) -> bool:
        """True if the given timestamp is a time instance value where
        there is a gap in information for the given data variable for 
        any of the datasets

        Args:
            ts (np.datetime64): time instance to be analysed
            data_vars (List[str]): data variables to be checked
            search_hours (Optional[List[int]]): used in a scenario where 
            the search should be executed in a specific set of hours, if 
            this option should not be used, it can be set to None. Default
            is None. (search_hours -> specific hours the user wants to 
            search, only at 6 AM and 12 PM, for example).

        Returns:
            bool: true if it is in the gap
        """
        for time_gap_container in map(lambda x: x.get_metadata().time_gap_container, self._repositories):
            if time_gap_container.is_gap(ts, data_vars, search_hours):
                return True
        return False

    def get_metadata_by_data_var(self, data_variable:str) -> RepositoryMetadata:
        """Returns the metadata of a repository that has the specific data_variable

        Args:
            data_variable (str): wanted data variable

        Raises:
            ValueError: if the repository is not found

        Returns:
            RepositoryMetadata: the metadata of the repository that has 
            the data variable
        """
        return self.get_repository_by_data_var(data_variable).get_metadata()

    def get_low_resolution_params_by_data_var(self, data_variable: str) -> Dict[str, int]:
        """Returns the low resolution parameter of a repository that has the specific data_variable

        Args:
            data_variable (str): wanted data variable

        Raises:
            ValueError: if the repository is not found

        Returns:
            Dict[str, int]: the low resolution parameters of a repository
            that has the data variable
        """
        return self.get_repository_by_data_var(data_variable).get_low_resolution_parameters()

    def get_repository_by_data_var(self, data_variable: str) -> RepositoryLayer:
        for repository in self._repositories:
            if data_variable in repository.get_metadata().data_vars:
                return repository
        raise ValueError("Data variable does not exist in any repository")