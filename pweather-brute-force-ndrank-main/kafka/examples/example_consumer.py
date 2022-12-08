"""This file is an example of how a consumer should be instantiated in 
python-kafka. It is not used anywhere in the code, it is just used as a
reference.
"""

import json
from kafka import KafkaConsumer, KafkaProducer # type: ignore
from typing import Final


HOST: Final = "localhost:9092"

producer: KafkaProducer = KafkaProducer(
    bootstrap_servers=HOST,
    value_serializer=lambda v: json.dumps(v).encode('utf8'),
    key_serializer= lambda v: json.dumps(v).encode('utf8')
)


consumer: KafkaConsumer = KafkaConsumer(
    client_id="client2",
    group_id = "group1",
    bootstrap_servers=HOST,
    value_deserializer=lambda v: json.loads(v.decode('utf8')),
    key_deserializer= lambda v: json.loads(v.decode('utf8')),
    max_poll_records=10
)

consumer.subscribe(topics=["raw-requests"])
consumer.subscription()

for record in consumer:
    print(record)