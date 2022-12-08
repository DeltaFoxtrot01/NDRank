import logging
from typing import Callable, Dict, List, Tuple, Type, Optional

from correlation_functions.main_structure import CorrelationFunction
from repository.repository_layer import RepositoryLayer
from service.service_main_structure import ServiceLayer

class ComponentInjector:
    """Component responsible for managing the existing different object types.
    This allows the injection of new object instances like repository or service 
    implementations in the place they are defined.
    
    This is an example of how it should be used:

    ```
    component_injector: ComponentInjector = ComponentInjector()

    ...

    @component_injector.inject_repository("some-tag")
    class RepositoryImplementation(RepositoryLayer):
        ...
    ```
    From this point, it is possible to get an instance of the "RepositoryImplementation"
    by using the method get_instance like this:

    ```
    repository = component_injector.get_repo_instance("some-tag",...)
    ```
    """

    def __init__(self) -> None:
        self._repo_container: Dict[str, Type[RepositoryLayer]] = {}
        self._service_container: Dict[str, Type[ServiceLayer]] = {}
        self._correlation_container: Dict[str, Type[CorrelationFunction]] = {}
        self._properties_path: Optional[str] = None

    def _save_repo_new_instance(self, name: str, repository: Type[RepositoryLayer]) -> None:
        if name in self._repo_container:
            raise ValueError("Repository instance with name: " + name + " already exists")
        else:
            self._repo_container[name] = repository

    def _save_service_new_instance(self, name: str, service: Type[ServiceLayer]) -> None:
        if name in self._service_container:
            raise ValueError("Service instance with name: " + name + " already exists")
        else:
            self._service_container[name] = service

    def _save_correlation_new_instance(self, name: str, correlation: Type[CorrelationFunction]) -> None:
        if name in self._correlation_container:
            raise ValueError("Correlation instance with name " + name + " already exists")
        else:
            self._correlation_container[name] = correlation

    def _get_repo_list(self) -> str:
        res: str = "Available repositories:"
        for repos in self._repo_container:
            res += "\n\t- " + repos
        return res

    def _get_service_list(self) -> str:
        res: str = "Available services:"
        for service in self._service_container:
            res += "\n\t- " + service
        return res

    def _get_correlation_function_list(self) -> str:
        res: str = "Available correlation functions:"
        for function in self._correlation_container:
            res += "\n\t- " + function
        return res

    def inject_repository(self, repo_name: str) -> Callable:
        return _repo_wrapper(repo_name,self)

    def inject_service(self, service_name: str) -> Callable:
        return _service_wrapper(service_name, self)
        
    def inject_correlation_function(self, correlation_function_name:str) -> Callable:
        return _correlation_wrapper(correlation_function_name, self)

    @property
    def properties_path(self) -> Optional[str]:
        return self._properties_path

    @properties_path.setter
    def properties_path(self, path: str) -> None:
        self._properties_path = path

    def get_repo_instance(self, name: str, dataset_path: str, index_file_name: str) -> RepositoryLayer:
        """Returns the instance of the given name of the repository

        Args:
            name (str): name of the repository
            dataset_path (str): folder path of the dataset
            index_file_name (str): name of the index file name (file that details the files that 
            constitute the dataset and its metadata)

        Raises:
            ValueError: if the "name" is not associated to any repository

        Returns:
            RepositoryLayer: respective repository
        """
        logging.debug("Requested repository with name " + name)
        if not name in self._repo_container:
            raise ValueError("Repository with name " + name + " was not found. " + self._get_repo_list())
        return self._repo_container[name](dataset_path,index_file_name)

    def get_service_instance(self, name: str, repositories: List[RepositoryLayer]) -> ServiceLayer:
        """Returns the instance of the given name of the service

        Args:
            name (str): name of the service
            repositories (List[RepositoryLayer]): repositories to be provided to the service

        Raises:
            ValueError: if the "name" is not associated to any service

        Returns:
            ServiceLayer: respective service
        """
        logging.debug("Requested service with name " + name)

        if not name in self._service_container:
            raise ValueError("Service with name " + name + " was not found. " + self._get_service_list())
        return self._service_container[name](repositories)

    def get_correlation_function_instance(self, name:str) -> CorrelationFunction:
        """Returns the instance of the given name of the correlation function

        Args:
            name (str): name of the correlation function

        Raises:
            ValueError: if the "name" is not associated to any correlation functino

        Returns:
            CorrelationFunction: respective correlation function
        """
        logging.debug("Requested correlation function with name " + name)
        if not name in self._correlation_container:
            raise ValueError("Correlation function with name " + name + " was not found. " + self._get_correlation_function_list())
        
        if self.properties_path is None:
            return self._correlation_container[name](name)
        else:
            return self._correlation_container[name](name, self.properties_path)

    def get_all_correlation_function_instances(self) -> List[Tuple[str,CorrelationFunction]]:
        """Returns an instance of all existing correlation functions

        Returns:
            List[Tuple[str,CorrelationFunction]]: correlation functions by name and instance
        """
        res: List[Tuple[str,CorrelationFunction]] = []
        for key in self._correlation_container.keys():
            if self._properties_path is None:
                res.append((key, self._correlation_container[key](key)))
            else:
                res.append((key, self._correlation_container[key](key,self._properties_path)))
        return res

class _wrapper:
    def __init__(self, name: str, injector: ComponentInjector) -> None:
        self._name: str = name
        self._component_injector: ComponentInjector = injector


class _repo_wrapper(_wrapper):
    """Auxiliary class for the ComponentInjector class for RepositoryLayer implementations
    """
    def __call__(self, repo: Type[RepositoryLayer]) -> Type[RepositoryLayer]:
        self._component_injector._save_repo_new_instance(self._name, repo)
        return repo

class _service_wrapper(_wrapper):
    """Auxiliary class for the ComoponentInjector class for ServiceLayer implementations
    """
    def __call__(self, service: Type[ServiceLayer]) -> Type[ServiceLayer]:
        self._component_injector._save_service_new_instance(self._name, service)
        return service

class _correlation_wrapper(_wrapper):
    """Auxliary class for the ComponentInjector class for CorrelationFunction implementations
    """
    def __call__(self, correlation: Type[CorrelationFunction]) -> Type[CorrelationFunction]:
        self._component_injector._save_correlation_new_instance(self._name, correlation)
        return correlation

component_injector: ComponentInjector = ComponentInjector()