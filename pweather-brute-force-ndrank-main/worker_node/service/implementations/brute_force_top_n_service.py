from typing import Dict, Iterator, List, Optional, Tuple
from correlation_functions.main_structure import CorrelationFunction
from service.constants import SIMPLE_TOP_N_SERVICE
from service.data_types import ResultContainer
from service.implementations.brute_force_service import BruteForceService
from auxiliar.component_injector import component_injector
from service.service_main_structure import HeuristicResult, RequestParameters

@component_injector.inject_service(SIMPLE_TOP_N_SERVICE)
class BruteForceTopNService(BruteForceService):

    def _insert_sorted(self, elems: List[Tuple[str,ResultContainer]], corr_function: CorrelationFunction):
        """Sorts the last element in a similar way to insertion sort.
        It assumes that the rest of the list is sorted

        Args:
            elems (List[Tuple[str,ResultContainer]]): list with the element to be sorted
            corr_function (CorrelationFunction): used correlation function
        """
        for i in range(len(elems)-1,0,-1):
            if corr_function.compare(elems[i-1][1].value /elems[i-1][1].sum_counter,
                    elems[i][1].value /elems[i][1].sum_counter):
                elems[i-1], elems[i] = elems[i], elems[i-1]
            else:
                break

    def _filter_results_by_number_results(self, all_results: Dict[str,ResultContainer], size_input: int,
        num_results: int, corr_function: CorrelationFunction) -> Dict[str, ResultContainer]:
        """Filters the calculated results by the given number of results

        Args:
            all_results (Dict[str,ResultContainer]): obtained results
            size_input (int): size of the input
            num_results (int): number of wanted results
            corr_function (CorrelationFunction): used correlation function

        Returns:
            Dict[str, ResultContainer]: results filtered by the num_results parameter
        """
        
        totals: List[Tuple[str,ResultContainer]] = []
        res: Dict[str, ResultContainer] = {}
        worst_sim_value: Optional[Tuple[str,ResultContainer]] = None 

        for timestamp in all_results:
            elem: Tuple[str,ResultContainer] = (timestamp, all_results[timestamp])
            if elem[1].sum_counter < size_input:
                res[elem[0]] = elem[1]
            elif worst_sim_value is None:
                worst_sim_value = elem
                totals.append(elem)
            else:   
                if corr_function.compare(worst_sim_value[1].value /worst_sim_value[1].sum_counter, 
                        elem[1].value /elem[1].sum_counter ) or len(totals) < num_results:
                    if len(totals) >= num_results:
                        totals.pop()
                    totals.append(elem)
                    self._insert_sorted(totals, corr_function)
                    worst_sim_value = totals[-1]

        for elem in totals:
            res[elem[0]] = elem[1]

        return res


    def execute_search(self, file_paths: Dict[str,List[str]], request_parameters: RequestParameters, corr_function: CorrelationFunction, 
        num_results: Optional[int] = None) -> Tuple[Dict[str, ResultContainer], int]:
        """Executes the full brute force search and only returns the best n results and the results
        where there is only a partial value

        Args:
            file_paths (Dict[str,List[str]]): files with the given input
            num_results (Optional[int]): number of wanted results

        Returns:
            Tuple[Dict[str, ResultContainer], int]: _description_
        """

        if num_results is None or num_results <= 0:
            raise ValueError("Number of results must be a positive number")
        
        res_full: Dict[str,ResultContainer]
        res: Dict[str, ResultContainer] = {}
        size_input: int
        res_full, size_input = super().execute_search(file_paths, request_parameters, corr_function)

        totals: List[Tuple[str,ResultContainer]] = []

        worst_sim_value: Optional[Tuple[str,ResultContainer]] = None 

        for timestamp in res_full:
            elem: Tuple[str,ResultContainer] = (timestamp, res_full[timestamp])
            if elem[1].sum_counter < size_input:
                res[elem[0]] = elem[1]
            elif worst_sim_value is None:
                worst_sim_value = elem
                totals.append(elem)
            else:   
                if corr_function.compare(worst_sim_value[1].value /worst_sim_value[1].sum_counter, 
                        elem[1].value /elem[1].sum_counter ) or len(totals) < num_results:
                    if len(totals) >= num_results:
                        totals.pop()
                    totals.append(elem)
                    self._insert_sorted(totals, corr_function)
                    worst_sim_value = totals[-1]

        for elem in totals:
            res[elem[0]] = elem[1]
        return res, size_input

    def execute_search_on_ts(self, result_iterator: Iterator[HeuristicResult], file_paths: Dict[str,List[str]], 
        request_parameters: RequestParameters, corr_function: CorrelationFunction, 
        num_results: Optional[int] = None) -> Tuple[Dict[str, ResultContainer], int]:
        
        if num_results is None or num_results <= 0:
            raise ValueError("Number of results must be a positive number")
        
        res_full: Dict[str,ResultContainer]
        res: Dict[str, ResultContainer] = {}
        size_input: int

        res_full, size_input = super().execute_search_on_ts(result_iterator, 
            file_paths, request_parameters, corr_function, num_results)
        
        res = self._filter_results_by_number_results(
                        res_full, size_input, num_results, corr_function)
        
        return res, size_input