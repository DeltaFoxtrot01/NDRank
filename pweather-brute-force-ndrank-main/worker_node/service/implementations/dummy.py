import logging
from typing import Dict, Iterator, List, Optional, Tuple
from auxiliar.component_injector import component_injector
from correlation_functions.main_structure import CorrelationFunction
from service.service_main_structure import HeuristicResult, RequestParameters, ServiceLayer
from service.constants import DEV_DUMMY_TAG
from repository.implementations.dummy import DummyRepository
from service.data_types import ResultContainer

@component_injector.inject_service(DEV_DUMMY_TAG)
class DummyService(ServiceLayer):
    """
    Dummy service used for testing other components

    """
    def __init__(self, repository = DummyRepository("/dummy-folder","settings.yaml")) -> None:
        super().__init__(repository)

    def execute_search(self, file_paths: Dict[str,List[str]], request_parameters: RequestParameters, corr_function: CorrelationFunction, num_results:Optional[int] = None) -> Tuple[Dict[str, ResultContainer],int]:
        return {}, 0

    def execute_search_on_ts(self, result_iterator: Iterator[HeuristicResult], file_paths: Dict[str,List[str]], request_parameters: RequestParameters, corr_function: CorrelationFunction, num_results:Optional[int] = None) -> Tuple[Dict[str, ResultContainer], int]:
        for result in result_iterator:
            logging.info("Received timestamp input: " + result.ts)
        return {}, 0