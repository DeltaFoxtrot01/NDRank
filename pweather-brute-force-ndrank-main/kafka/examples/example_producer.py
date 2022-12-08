"""This file is an example of how a producer should be instantiated in 
python-kafka. It is not used anywhere in the code, it is just used as a
reference.
"""
import json
from kafka import KafkaProducer # type: ignore
from typing import Final


HOST: Final = "localhost:9092"

producer: KafkaProducer = KafkaProducer(
    bootstrap_servers=HOST,
    value_serializer=lambda v: json.dumps(v).encode('ascii'),
    key_serializer= lambda v: json.dumps(v).encode('ascii')
)

producer.send(
    "raw-requests",
    key={"request_id": "request-1", "node_id": "node-1"},
    value=
    {
        "final-results": [("saldfjdas",0.875)],
        "partial-results": []
    }
)

producer.flush()