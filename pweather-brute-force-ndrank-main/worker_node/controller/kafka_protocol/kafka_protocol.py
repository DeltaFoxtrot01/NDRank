import json, logging
from typing import Any, Dict, Final, Iterator, List, Optional
import numpy as np
from schema import Schema #type: ignore
from protocol.protocol_pb2 import SearchRequest

from kafka import KafkaProducer #type: ignore
from kafka.producer.future import FutureRecordMetadata #type: ignore
from kafka import KafkaConsumer, TopicPartition #type: ignore
from kafka.consumer.fetcher import ConsumerRecord #type: ignore

from service.data_types import CandidateContainer, ResultContainer
from service.service_main_structure import HeuristicResult

"""The code in this package encapsulates the communication 
between the worker and the kafka queue.
"""

logger = logging.getLogger('kafka')
logger.setLevel(logging.WARNING)

ENCODING: Final = "utf-8"
NEW_REQUESTS_TOPIC: Final = "raw-requests"
NEW_CANDIDATES_TOPIC: Final = "raw-candidates"
KAFKA_PREFIX_TOPIC: Final = "candidate-"

SIZE_INPUT: Final = "size-input"
NUM_RESULTS: Final = "num-results"

FINAL_RESULT_TAG: Final = "final-results"
PARTIAL_RESULT_TAG: Final = "partial-results"

REQUEST_ID: Final = 'request_id'
NODE_ID: Final = 'node_id'

TIMESTAMP: Final = "timestamp"
VALUE: Final = "value"
BEST_VALUE: Final = "best-value"
WORST_VALUE: Final = "worst-value"
SUM_COUNTER: Final = "sum-counter"

SIZE_KAFKA_MESSAGE: Final = 100 * (1024**2)

IS_REVERSE_ORDER: Final = "is-reserve-order"

CLIENT_ID: Final = "0"

value_schema: Schema = Schema(
    [{
        TIMESTAMP: str,
        VALUE: float
    }]
)

candidate_schema: Schema = Schema(
    [{
        TIMESTAMP: str,
        BEST_VALUE: float,
        WORST_VALUE: float
    }]
)


def serialize(value: Any) -> bytes:
    """Used serialization method for kafka

    Args:
        value (Any): python object to be serialized

    Returns:
        bytes: object serialized
    """
    try:
        return json.dumps(value).encode(ENCODING)
    except Exception as e:
        logging.exception("Exception raised in serialize method for kafka")
        raise e

def deserialize(value: bytes) -> Any:
    """Used deserialization method for kafka

    Args:
        value (bytes): serialized object

    Returns:
        Any: deserialized python object
    """
    try:
        return json.loads(value.decode(ENCODING))
    except Exception as e:
        logging.exception("Exception raised in deserialize method for kafka")
        raise e

class KafkaProtocol:
    """Class used to establish a channel between the worker node and 
    the kafka queue
    """
    
    def __init__(self, kafka_host: str, kafka_port: int) -> None:
        self._kafka_host_name: str = kafka_host + ":" + str(kafka_port)

        self._kafka_producer: KafkaProducer = KafkaProducer(
            bootstrap_servers=self._kafka_host_name,
            value_serializer=lambda v: serialize(v),
            key_serializer= lambda v: serialize(v),
            max_request_size=SIZE_KAFKA_MESSAGE,
            buffer_memory=SIZE_KAFKA_MESSAGE
        )

    def convert_results_to_kafka_object(self, results: Dict[str, ResultContainer], input_size: int,
         num_results: int, is_reverse_order: bool) -> Dict:
        """Converts the ResultContainer object to the defined structure for the kafka message queue

        Args:
            results (Dict[str, ResultContainer]): message with the results that should be sent
            input_size (int): size of the input provided for the search
            num_results (int): number of the results the request provided as an input
            is_reverse_order (bool): if the sorting order for similarity should be reversed

        Returns:
            Dict: object converted into a format usable by kafka
        """
        res: Dict = {}
        final_resuls: List[Dict[str, Any]] = []     # has the timestamp, the final value
        partial_results: List[Dict[str, Any]] = []  # has the timestamp, the current value and sum counter

        for key in results:
            container: ResultContainer = results[key]
            if container.sum_counter == input_size:
                final_resuls.append({TIMESTAMP: key, VALUE:container.value/input_size})
            else:
                partial_results.append({TIMESTAMP: key, VALUE: container.value, SUM_COUNTER: container.sum_counter})
        
        res[FINAL_RESULT_TAG] = final_resuls
        res[PARTIAL_RESULT_TAG] = partial_results
        res[SIZE_INPUT] = input_size
        res[NUM_RESULTS] = num_results
        res[IS_REVERSE_ORDER] = is_reverse_order
        
        return res


    def convert_candidates_to_kafka_object(self, candidates: Dict[np.datetime64, CandidateContainer], input_size: int, 
        num_results:int, is_reverse_order: bool) -> Dict:
        """Converts the CandidateContainer object to the defined structure for the kafka message queue

        Args:
            results (Dict[str, CandidateContainer]): message with the candidates that should be sent
            input_size (int): size of the input provided for the search
            num_results (int): number of the results the request provided as an input
            is_reverse_order (bool): if the sorting order for similarity should be reversed

        Returns:
            Dict: object converted into a format usable by kafka
        """
        res: Dict = {}
        final_candidates: List[Dict[str, Any]] = []     # has the timestamp, the final value
        partial_candidates: List[Dict[str, Any]] = []  # has the timestamp, the current value and sum counter

        for key in candidates:
            container: CandidateContainer = candidates[key]
            if container.is_final:
                final_candidates.append(
                    {
                        TIMESTAMP: str(key), 
                        BEST_VALUE: container.best_value, 
                        WORST_VALUE: container.worst_value})
            else:
                partial_candidates.append(
                    {
                        TIMESTAMP: str(key), 
                        BEST_VALUE: container.best_value,
                        WORST_VALUE: container.worst_value,
                        SUM_COUNTER: container.sum_counter})
        
        res[FINAL_RESULT_TAG] = final_candidates
        res[PARTIAL_RESULT_TAG] = partial_candidates
        res[SIZE_INPUT] = input_size
        res[NUM_RESULTS] = num_results
        res[IS_REVERSE_ORDER] = is_reverse_order
        
        return res

    def submit_result(self, request_id: str, message: Dict, node_id: str) -> None:
        """Submits the results message to kafka message queue

        Args:
            request_id (str): id of the request
            message (Dict): message to be submitted
            node_id (str): id of the node
        """
        send_res: FutureRecordMetadata = self._kafka_producer.send(
            NEW_REQUESTS_TOPIC,
            key={
                REQUEST_ID: request_id,
                NODE_ID: node_id
            },
            value=message
        )
        logging.debug("Producing message for request id: " + request_id)
        self._kafka_producer.flush(10)
        send_res.get()
        logging.debug("Message produced for request id: " + request_id)

    def submit_candidates(self, request_id: str, message: Dict, node_id: str) -> None:
        """Submits the candidates message to kafka message queue

        Args:
            request_id (str): id of the request
            message (Dict): message to be submitted
            node_id (str): id of the node
        """
        send_res: FutureRecordMetadata = self._kafka_producer.send(
            NEW_CANDIDATES_TOPIC,
            key={
                REQUEST_ID: request_id,
                NODE_ID: node_id
            },
            value=message
        )
        logging.debug("Producing candidates message for request id: " + request_id)
        self._kafka_producer.flush(10)
        send_res.get()
        logging.debug("Candidates message produced for request id: " + request_id)

    def get_results_kafka(self, request: SearchRequest) -> Iterator[HeuristicResult]:
        """Receives the results of a specific request and iterates 
        all received values one by one

        Args:
            request (SearchRequest): received request

        Yields:
            Iterator[HeuristicResult]: all received responses
        """
        consumer: KafkaConsumer = KafkaConsumer(
            bootstrap_servers=self._kafka_host_name,
            group_id = None,
            client_id = CLIENT_ID,
            value_deserializer=lambda v: deserialize(v),
            key_deserializer= lambda v: deserialize(v),
            max_partition_fetch_bytes=SIZE_KAFKA_MESSAGE,
            fetch_max_bytes=SIZE_KAFKA_MESSAGE
        )

        consumer.assign([TopicPartition(str(request.request_id), 0)])
        consumer.seek_to_beginning()

        logging.debug("Subscribed to topic " + request.request_id + ". Waiting...")
        records: ConsumerRecord 
        try:
            records = next(consumer)
        except Exception as e:
            logging.exception(e)
        logging.debug("Received a Kafka message from topic " + request.request_id)
        result: Optional[List[Dict[str,Any]]] = records.value

        if result is None:
            raise ValueError("Value received when subscribing to kafka topic came null")

        value_schema.validate(result)

        consumer.unsubscribe()
        consumer.close()

        return map(lambda elem: HeuristicResult(elem[TIMESTAMP], elem[VALUE]), result)

    def get_candidates_kafka(self, request: SearchRequest) -> Iterator[HeuristicResult]:
        """Receives the candidates of a specific request and iterates 
        all received values one by one

        Args:
            request (SearchRequest): received request

        Yields:
            Iterator[HeuristicResult]: all received responses
        """
        consumer: KafkaConsumer = KafkaConsumer(
            bootstrap_servers=self._kafka_host_name,
            group_id = None,
            client_id = CLIENT_ID,
            value_deserializer=lambda v: deserialize(v),
            key_deserializer= lambda v: deserialize(v),
        )

        topic_id: str = KAFKA_PREFIX_TOPIC + str(request.request_id)

        consumer.assign([TopicPartition(topic_id, 0)])
        consumer.seek_to_beginning()

        logging.debug("Subscribed to topic " + topic_id + ". Waiting...")
        records: ConsumerRecord 
        try:
            records = next(consumer)
        except Exception as e:
            logging.exception(e)
        logging.debug("Received a Kafka message from topic " + topic_id)
        result: Optional[List[Dict[str,Any]]] = records.value

        if result is None:
            raise ValueError("Value received when subscribing to kafka topic came null")

        candidate_schema.validate(result)

        consumer.unsubscribe()
        consumer.close()

        logging.debug(result)

        return map(lambda elem: HeuristicResult(elem[TIMESTAMP], elem[BEST_VALUE], elem[WORST_VALUE]), result)