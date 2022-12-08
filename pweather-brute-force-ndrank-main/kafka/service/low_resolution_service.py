from concurrent.futures import thread
import logging
import threading
from typing import Any, Dict, List, Optional, Set
from dtos.consumer_message import BEST_VALUE, SUM_COUNTER, TIMESTAMP, VALUE, WORST_VALUE, CandidatesConsumerMessage, ResultsConsumerMessage
from dtos.producer_message import ProducerMessage, ProducerMessage
from service.service import ServiceBase

class _totalResult:
    def __init__(self, ts: str, value: float) -> None:
        self._ts: str = ts
        self._value: float = value

    @property
    def ts(self) -> str:
        return self._ts

    @property
    def value(self) -> float:
        return self._value

class _partialResult:
    def __init__(self, value: float, sum_counter: int) -> None:
        self._value: float = value
        self._sum_counter: int = sum_counter

    def sum_new_values(self, value: float, sum_counter) -> None:
        self._value += value
        self._sum_counter += sum_counter

    @property
    def value(self) -> float:
        return self._value

    @property
    def sum_counter(self) -> int:
        return self._sum_counter

class _totalCandidate:
    def __init__(self, ts: str, best_value: float, worst_value: float) -> None:
        self._ts: str = ts
        self._best_value: float = best_value
        self._worst_value: float = worst_value

    @property
    def ts(self) -> str:
        return self._ts

    @property
    def best_value(self) -> float:
        return self._best_value

    @property
    def worst_value(self) -> float:
        return self._worst_value

class _partialCandidate:
    def __init__(self, best_value: float, worst_value: float, sum_counter: int) -> None:
        self._best_value: float = best_value
        self._worst_value: float = worst_value
        self._sum_counter: int = sum_counter
    
    def sum_new_values(self, best_value: float, worst_value: float, sum_counter: int) -> None:
        self._best_value += best_value
        self._worst_value += worst_value
        self._sum_counter += sum_counter

    @property
    def best_value(self) -> float:
        return self._best_value

    @property
    def worst_value(self) -> float:
        return self._worst_value

    @property
    def sum_counter(self) -> int:
        return self._sum_counter


class ResultsContainer:
    """Container to store all received the heuristic messages
    and merges them together until the final message is received
    """

    def __init__(self, message: ResultsConsumerMessage) -> None:
        self._total_messages: List[_totalResult] = \
            self._map_total_values(message.results)
            
        self._partial_results: Dict[str,_partialResult] = {}
        self._size_input: int = message.size_input
        self._nodes_received: Set[str] = set()

        self._nodes_received.add(message.node_id)

        self._add_partial_results(message.partial_results)

    def _map_total_values(self, values: List[Dict[str, Any]]) -> List[_totalResult]:
        return list(
            map(lambda elem: _totalResult(elem[TIMESTAMP], elem[VALUE]), values)
        )

    def _add_partial_results(self, partial_res: List[Dict[str, Any]]) -> None:
        for elem in partial_res:
            if elem[TIMESTAMP] in self._partial_results:
                self._partial_results[elem[TIMESTAMP]].sum_new_values(
                    elem[VALUE], elem[SUM_COUNTER]
                )
                if self._partial_results[elem[TIMESTAMP]].sum_counter == self._size_input:
                    self._total_messages.append(
                        _totalResult(elem[TIMESTAMP], elem[VALUE] / elem[SUM_COUNTER])
                    )
                    self._partial_results.pop(elem[TIMESTAMP])
            else:
                self._partial_results[elem[TIMESTAMP]] = \
                    _partialResult(elem[VALUE], elem[SUM_COUNTER])

    @property
    def node_ids(self) -> Set[str]:
        return self._nodes_received

    def add_message(self, message: ResultsConsumerMessage) -> None:
        self._total_messages += self._map_total_values(message.results)
        self._add_partial_results(message.partial_results)
        self._nodes_received.add(message.node_id)


    def get_ProducerMessage_value(self, num_results: int, req_id: str, is_reverse_order: bool) -> ProducerMessage:
        res: ProducerMessage
        
        self._total_messages.sort(key=lambda x: x.value, reverse=is_reverse_order)

        res = ProducerMessage(req_id,list(
            map(lambda elem: {TIMESTAMP: elem.ts, VALUE: elem.value}, 
            self._total_messages[:num_results])))

        return res

class CandidateContainer:
    """Container that stores all the candidates that have been received
    and merges them together as they are added
    """
    def __init__(self, message: CandidatesConsumerMessage) -> None:
        self._list: List[_totalCandidate] = []
        self._partial_candidates: Dict[str,_partialCandidate] = {}

        self._top_res: int = message.num_results
        self._is_reverse: bool = message.is_reverse_order
        self._input_size: int = message.size_input
        self._req_id: str = message.request_id

        self._nodes_received: Set[str] = set()

        self.add_message(message)

    def _add_total_candidates(self, message: CandidatesConsumerMessage) -> None:
        self._list += list(map(
            lambda x : _totalCandidate(x[TIMESTAMP], x[BEST_VALUE], x[WORST_VALUE]), 
            message.results 
        ))

    def _add_partial_candidates(self, message: CandidatesConsumerMessage) -> None:
        for candidate in message.partial_results:
            if candidate[TIMESTAMP] in self._partial_candidates:
                self._partial_candidates[candidate[TIMESTAMP]].sum_new_values(
                    candidate[BEST_VALUE],
                    candidate[WORST_VALUE],
                    candidate[SUM_COUNTER])
            else:
                self._partial_candidates[candidate[TIMESTAMP]] =\
                    _partialCandidate(
                    candidate[BEST_VALUE],
                    candidate[WORST_VALUE],
                    candidate[SUM_COUNTER])
            if self._partial_candidates[candidate[TIMESTAMP]].sum_counter == self._input_size:
                self._list.append(_totalCandidate(
                    candidate[TIMESTAMP], 
                    candidate[BEST_VALUE]/self._input_size,
                    candidate[WORST_VALUE]/self._input_size)
                )
    
    @property
    def node_ids(self) -> Set[str]:
        return self._nodes_received

    def add_message(self, message: CandidatesConsumerMessage) -> None:
        self._add_total_candidates(message)
        self._add_partial_candidates(message)
        self._nodes_received.add(message.node_id)
    
    def get_ProducerMessage_value(self) -> ProducerMessage:
        if len(self._list) < self._top_res:
            return ProducerMessage(self._req_id, list(map(
                        lambda x: {
                            TIMESTAMP: x.ts, 
                            BEST_VALUE: x.best_value, 
                            WORST_VALUE: x.worst_value
                        }, self._list)))
        
        self._list.sort(key = lambda x: x.worst_value, reverse=self._is_reverse)
        n_element: _totalCandidate = self._list[self._top_res - 1]
        
        logging.debug("Total size: " + str(len(self._list)))
        for i in range(len(self._list)-1, self._top_res, -1):
            if (self._list[i].best_value < n_element.worst_value and self._is_reverse) or \
               (self._list[i].best_value > n_element.worst_value and not self._is_reverse):
                self._list.pop(i)
        logging.debug("-------------------------------------------------")
        logging.debug("Total size: " + str(len(self._list)))

        return ProducerMessage(self._req_id, list(map(
            lambda x: {
                TIMESTAMP: x.ts, 
                BEST_VALUE: x.best_value, 
                WORST_VALUE: x.worst_value
            }, self._list)))
    

class LowResolutionService(ServiceBase):
    """Deals with the responses received from the low resolution search

    In the end merges all responses together in one final response.
    """
    def __init__(self, node_ids: List[str]) -> None:
        super().__init__()
        self._node_ids: List[str] = node_ids
        self._current_requests: Dict[str,ResultsContainer] = {} # stores all received messages organized by request_id
        self._current_candidates: Dict[str, CandidateContainer] = {}
        self._results_lock: threading.Lock = threading.Lock()
        self._candidates_lock: threading.Lock = threading.Lock()
        
    def process_results_message(self, message: ResultsConsumerMessage) -> Optional[ProducerMessage]:
        """Processes the received results messages from the search on the low resolution dataset

        Args:
            message (ResultsConsumerMessage): Message received by the message queue

        Raises:
            ValueError: if two messages are received for the same request from the same node

        Returns:
            Optional[ProducerMessage]: a Producer message if all nodes have sent a message for the given
            request with the top n results sorted by similarity value or None otherwise
        """
        logging.info("Received a results message of size " + str(len(message.results)) + 
        " from node " + str(message.node_id) + "for request " + message.request_id)
        
        req_id: str = message.request_id

        self._results_lock.acquire()
        if not req_id in self._current_requests:
            self._current_requests[req_id] = ResultsContainer(message)
        else: 
            if not message.node_id in self._current_requests[req_id].node_ids:
                self._current_requests[req_id].add_message(message)
            else:
                raise ValueError("Received a duplicated message for request id " + \
                    req_id + " from node id " + message.node_id)

        if len(self._current_requests[req_id].node_ids) == len(self._node_ids):
            aux: ResultsContainer = \
                self._current_requests.pop(req_id)
            self._results_lock.release()

            return aux.get_ProducerMessage_value(message.num_results, message.request_id, message.is_reverse_order)
        else:
            self._results_lock.release()

        return None

    def process_candidates_message(self, message: CandidatesConsumerMessage) -> Optional[ProducerMessage]:
        """Processes the message coming from the candidate worker nodes

        Args:
            message (CandidatesConsumerMessage): candidate message

        Raises:
            ValueError: if a second message from the same worker for the same request is receiveds

        Returns:
            Optional[ProducerMessage]: either a result with all messages merged, or None if it there
            are still messages missing
        """
        logging.info("Received a candidates message of size " + str(len(message.results)) + 
        " from node " + str(message.node_id) + " for request " + message.request_id)

        req_id: str = message.request_id

        self._candidates_lock.acquire()
        if not req_id in self._current_candidates:
            self._current_candidates[req_id] = CandidateContainer(message)
        else:
            if not message.node_id in self._current_candidates[req_id].node_ids:
                self._current_candidates[req_id].add_message(message)
            else:
                raise ValueError("Received a duplicate candidate message for request id " + \
                    req_id + " from node id " + message.node_id)
        if len(self._current_candidates[req_id].node_ids) == len(self._node_ids):
            aux: CandidateContainer = \
                self._current_candidates.pop(req_id)
            self._candidates_lock.release()

            return aux.get_ProducerMessage_value()
        else:
            self._candidates_lock.release()

        return None