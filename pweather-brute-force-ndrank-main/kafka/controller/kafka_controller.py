import json
import logging
import threading
from typing import Dict, Final, List, Optional
from kafka import KafkaConsumer, KafkaProducer, TopicPartition #type: ignore
from kafka.producer.future import FutureRecordMetadata #type: ignore
from kafka.consumer.fetcher import ConsumerRecord #type: ignore
from controller.controller import ControllerBase
from dtos.consumer_message import CandidatesConsumerMessage, ResultsConsumerMessage
from dtos.producer_message import ProducerMessage
from service.service import ServiceBase

ENCODING: Final = "utf-8"
MAX_POLL_RECORDS: Final = 10
NEW_REQUESTS_TOPIC: Final = "raw-requests"
NEW_CANDIDATES_TOPIC: Final = "raw-candidates"

SIZE_KAFKA_MESSAGE: Final = 100 * (1024**2)


logger = logging.getLogger('kafka')
logger.setLevel(logging.WARNING)

class KafkaController(ControllerBase):
    """Controller that processes the messages coming from the kafka queue"""
    def __init__(self, service: ServiceBase, host: str, port: int, 
                 group_id: str, client_id: str):
        super().__init__(service)
        host_plus_port: str = host + ":" + str(port)
        self._consumer: KafkaConsumer = \
            KafkaConsumer(
                bootstrap_servers=host_plus_port,
                client_id=client_id,
                group_id=group_id,
                value_deserializer=lambda v: json.loads(v.decode(ENCODING)),
                key_deserializer= lambda v: json.loads(v.decode(ENCODING)),
                max_poll_records=MAX_POLL_RECORDS,
                consumer_timeout_ms=5000
                #buffer_memory=SIZE_KAFKA_MESSAGE
            )
        self._producer: KafkaProducer = \
            KafkaProducer(
                bootstrap_servers=host_plus_port,
                value_serializer=lambda v: json.dumps(v).encode(ENCODING),
                key_serializer= lambda v: json.dumps(v).encode(ENCODING),
                max_request_size=SIZE_KAFKA_MESSAGE,
                buffer_memory=SIZE_KAFKA_MESSAGE
            )
        self._consumer.subscribe(topics=[NEW_REQUESTS_TOPIC, NEW_CANDIDATES_TOPIC])
        self._lock: threading.Lock = threading.Lock()
        self._is_stopped: bool = False

    def _produce_result(self, message: ProducerMessage, prefix: str = "") -> None:
        used_topic:str = prefix + message.request_id
        logging.info("Going to produce a topic: " + used_topic)
        logging.debug("Object: " + str(message))
        send_res: FutureRecordMetadata = self._producer.send(
            used_topic,
            message.values
        )
        self._producer.flush()
        send_res.get()
        logging.info("Topic produced")

    def start(self) -> None:
        """
        Listens to new messages from the 'NEW_REQUESTS_TOPIC' or from 'NEW_CANDIDATES_TOPIC'
        """
        processed_response: Optional[ProducerMessage] = None
        self._lock.acquire()
        while not self._is_stopped:
            self._lock.release()
            records: Dict[TopicPartition, List[ConsumerRecord]] = self._consumer.poll(5000)
            
            for key in records:
                #This statement verifies what kind of message it is (if it is a heuristic message
                # or a candidates message), and submits to the right method of the service
                if NEW_REQUESTS_TOPIC in key[0]:
                    for message in records[key]:
                        processed_response = \
                            self._service.process_results_message(ResultsConsumerMessage(message.key, message.value))
                        if not processed_response is None:
                            self._produce_result(processed_response)
                elif NEW_CANDIDATES_TOPIC in key[0]:
                    for message in records[key]:
                        processed_response = \
                            self._service.process_candidates_message(CandidatesConsumerMessage(message.key, message.value))
                        if not processed_response is None:
                            self._produce_result(processed_response, "candidate-")
                    
            self._lock.acquire()

        self._lock.release()

    def close(self) -> None:
        """Closes both the consumer and producer for the kafka queue
        """
        logging.info("Closing kafka consumer and producer")
        self._lock.acquire()
        self._consumer.unsubscribe()
        self._consumer.close()
        self._producer.close()
        self._is_stopped = True
        self._lock.release()