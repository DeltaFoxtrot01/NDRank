# kafka_merger (Queue Middle Man)
This component reads the messages submitted by the worker nodes into the kafka queue, merges these messages together into a single final response and resubmits back into to the message queue. It deals with the results returned by the search on the low resolution dataset and the candidates processed by the workers.

## Available flags:
When executing the server, the follow flags can be passed:
- -p (path of the properties file)

The ```-p``` allows to override the default "properties.yaml" file path. This flag is optional.

## properties.yaml structure
### Kafka configurations 

The connection to kafka can be configured with the following block:
```
network-config:
  ip: <IP of the kafka server>
  port: <Port of the kafka server>
kafka:
  group-id: <group of id for kafka>
  client-id: <client id for kafka>
```
The ```group-id``` and ```client-id``` are kafka specific. The group id is used as an Id for a group of nodes and the client id is the id of each node in the group. This allows a parallel processing of different topics. For this project this is not essencial, but a configuration must always be provided.

### node ids
The ```node-ids``` is the list of ids of the worker nodes. It is structured in the following format:
```
node-ids:
  - <node id>
  - <another node id>
```

These ids are used to make sure that all messages have been received from all workers and that there are no duplicates.