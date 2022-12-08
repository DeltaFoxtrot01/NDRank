from typing import Any, Dict, Final, List, Tuple
from schema import Schema, Or #type: ignore

FINAL_RESULT_TAG: Final = "final-results"
PARTIAL_RESULT_TAG: Final = "partial-results"

SIZE_INPUT: Final = "size-input"
NUM_RESULTS: Final = "num-results"

REQUEST_ID: Final = 'request_id'
NODE_ID: Final = 'node_id'

TIMESTAMP: Final = "timestamp"
VALUE: Final = "value"
BEST_VALUE: Final = "best-value"
WORST_VALUE: Final = "worst-value"
SUM_COUNTER: Final = "sum-counter"

IS_REVERSE_ORDER: Final = "is-reserve-order"

key_schema: Schema = Schema(
    {
        REQUEST_ID: str,
        NODE_ID: str
    }
)

value_schema: Schema = Schema(
    {
        FINAL_RESULT_TAG: [{
            TIMESTAMP: str,
            VALUE: float
        }],
        PARTIAL_RESULT_TAG: [{
            TIMESTAMP: str,
            VALUE: float,
            SUM_COUNTER: int
        }],
        SIZE_INPUT: int,
        NUM_RESULTS: int,
        IS_REVERSE_ORDER: bool
    }
)

candidate_schema: Schema = Schema(
    {
        FINAL_RESULT_TAG: [{
            TIMESTAMP: str,
            BEST_VALUE: float,
            WORST_VALUE: float
        }],
        PARTIAL_RESULT_TAG: [{
            TIMESTAMP: str,
            BEST_VALUE: float,
            WORST_VALUE: float,
            SUM_COUNTER: int
        }],
        SIZE_INPUT: int,
        NUM_RESULTS: int,
        IS_REVERSE_ORDER: bool
    }
)

class ConsumerMessage:
    """Dto of the message received from the message queue"""
    def __init__(self, key: Dict, value: Dict):
        """Basic constructor

        Args:
            key (Dict): received key with the format of "key_schema"
            value (Dict): received values with the format of "value_schema"

        Raises:
            TypeError: Raised if the given objects do not match the schema
        """
        self._request_id: str = key[REQUEST_ID]
        self._node_id: str = key[NODE_ID]
        self._results: List[Dict[str, Any]]= value[FINAL_RESULT_TAG]
        self._partial_results: List[Dict[str, Any]] = value[PARTIAL_RESULT_TAG]
        self._size_input: int = value[SIZE_INPUT]
        self._num_results: int = value[NUM_RESULTS]
        self._is_reverse_order: bool = value[IS_REVERSE_ORDER]

    @property
    def request_id(self) -> str:
        return self._request_id

    @property
    def partial_results(self) -> List[Dict[str, Any]]:
        return self._partial_results

    @property
    def results(self) -> List[Dict[str, Any]]:
        return self._results

    @property
    def node_id(self) -> str:
        return self._node_id   

    @property
    def size_input(self) -> int:
        return self._size_input

    @property
    def num_results(self) -> int:
        return self._num_results
    
    @property
    def is_reverse_order(self) -> bool:
        return self._is_reverse_order

    def __str__(self) -> str:
        return self._request_id + " from " + self._node_id + ": " + str(self._results)

class ResultsConsumerMessage(ConsumerMessage):

    def __init__(self, key: Dict, value: Dict):
        if not value_schema.validate(value) or not key_schema.validate(key):
            raise TypeError("either the key or the value are not valid according to the given schemas")

        super().__init__(key, value)



class CandidatesConsumerMessage(ConsumerMessage):
    
    def __init__(self, key: Dict, value: Dict) -> None:
        if not candidate_schema.validate(value) or not key_schema.validate(key):
            raise TypeError("either the key or the value are not valid according to the given schemas")

        super().__init__(key, value)