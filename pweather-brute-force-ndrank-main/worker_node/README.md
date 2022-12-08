# Worker Node
The worker node represents both the worker nodes for the brute force architecture and NDRank's architecture.

The behaviour is determined by the configurations provided in the "properties.yaml" file.

## Worker Architecture
The Worker node hes 3 main components: 
- The controller
- The service
- The repository 

The controller is the group of classes responsible for handling the comunication with the Kafka queue and the master mode. The communication between the workers end the master are implemented using gRPC for most operations and plain TCP for file transfer. The communication with the Kaffa queue is handled with the "Kafka-python" package. 

The service is the group of classes responsible for executing the search on the local portion of the dataset.

The repository is the group of classes responsible for the data access to the local portions of the datasets, either the low resolution dataset or the full resolution dataset.

## Available flags:

To execute a worker run the following command:

```
python3 __main__.py -d <boolean value> -p <path of the properties file>
```

When executing the server, the follow flags can be passed:
- -d (should delete files)
- -p (path of the properties file)

The ```-d``` allows to determine if the files created in the temporary folder by the file transfer protocol should be deleted or not. The default is "false", "true" can be passed to override this behaviour.

The ```-p``` allows to override the default "properties.yaml" file path. This flag is optional.

## properties.yaml structure

This is the basic structure of the properties file:
```
node-id: <id of the node>
network-config:
  ip: <local ip to attach grpc>
  port: <port for grpc>
  kafka-ip: <ip of kafka queue>
  kafka-port: <port of kafka queue>
  available_ports:
    from: <lower limit for ports that can be used by TCP sockets for file transfer>
    to: <upper limit for ports that can be used by TCP sockets for file transfer>
temporary-folder: <path for temporary folder>
controller: <name of the controller>
service: <name of the service>
repository:
  type: <name of the repository>
  paths: 
    - <path of repository>
    - <path of repository>
    ...
low-resolution-service: <name of the low resolution service>
low-resolution-repository:
  type: <name of the low resolution repository>
  paths: 
    - <path of low resolution repository>
    - <path of low resolution repository>
    ...
correlation-functions:
  average-path: <path for average parameters>
  standard-deviation-path: <path for standard deviation parameters>
```

### Network configuration:
To configure basic network configurations, the following block is used:
```
network-config:
  ip: <host ip>                 #IP the server should be used by the server's sockets
  port: <host port>             #Port for the gRPC server
  kafka-ip: <kafka ip address>  #IP of the kafka server
  kafka-port: <kafka port>      #Port of the kafka server
  available_ports:
    from: <from port>           #Interval of ports that can be used for the file
    to: <to port>               #tranfer sockets
```
The configurations for the gRPC server are done through the ```ip``` and the ```port``` tags.

The kafka tags configure the connection to the kafka queue and are not necessary for the brute force worker node.

The ```available_ports``` are used to configure the ports that the TCP sockets can use. This is a part of the protocol that is responsible for the transfer of the input files. It uses the same IP as the gRPC IP.

### Controller
The following tag controlls what is going to be the server role as a worker:
```
controller: <controller name>
```

There are two available values for the ```controller``` tag:
 - "brute-force" (behaves like a brute-force worker node)
 - "ndrank" (behaves like a NDRank worker node)

The "brute-force" controller executes the system in a brute force mode. This mode consists on receiving a request from the master, executing the search locally and then returning the result to the master. 

If it is necessary to use the Kafka queue, then the "ndrank" con­troller is necessary. The ndrank con­troller receives the request from the master, executes the search on the low resolution dataset and submits the results to the Kafka queue. The results from the Kafka queue are then used in a search that is executed by timestamp (instead of searching the full portion, only the regions indicated by the results are searched. 

The ndrank controller can receive two different services: one for the low resolution dataset and one for the full resolution dataset. For the situation where there is no low resolution dataset, the same service is used for both steps, where in the first step a search for candidates is executed. 

### Service
The way the services can be configured is by using the following tags:
```
service: <service name>
...
low-resolution-service: <name of the low resolution service>
```

The ```service``` tag is used to configure the service for the brute-force worker and the ndrank worker for search in the full resolution dataset.

The ```low-resolution-service``` tag is used by the ndrank node to configure the service used to search on the low resolution dataset. To pass no service for the low resolution dataset to a ndrank worker, pass an empty string like ```""```.

The available services are:
- "simple-service" (used with the developement dataset, executes either a simple brute force search or just on heuristic results)
- "simple-top-n-service" (same as before, but it only returns the top n results)
- "dummy-service" (does nothing)
- "parameter-candidate-list-service" (executes a search by first creating a list of candidates)

All implementations can be found under the folder ```service/implementations```.

To add a new service, a new ```.py``` file must be created under that directory.

The class should inherit the ```ServiceLayer``` class and be decorated with the ```@component_injector.inject_service(<used tag>)``` decorator, in order to inject the new implementation. The ```dummy.py``` can be used as an example.

#### simple-service
The simple-service executes a brute force search. It also allows the search to be executed by timestamp. 

#### simple-top-n-service 
Executes the simple-service, but only returns the top n results.

#### dummy-service 
Returns no results. 

#### parameter-candidate-list-service 
Executes the search for candidates to be in the find list of results. So, instead of returning a single similarity value, two values are returned where one is the best possible similarity value and the second is the worst possible similarity value.

### Repository
The repository is configured with the following tags:
```
repository:
  type: <repository name>
  paths: 
    - <path of repository>
    - <path of repository>
    ...
...
low-resolution-repository:
  type: <name of the low resolution repository>
  paths: 
    - <path of low resolution repository>
    - <path of low resolution repository>
    ...
```
The ```type``` tag determines the kind of repository that should be used. The ```paths``` tag should point to the paths where the local portion of the dataset can be found. A file called ```settings.yaml``` with the required data to index the dataset is expected.

The ```repository``` configures the repositories for the brute force worker and the full resolution dataset for the ndrank worker. The ```low-resolution-repository``` configures the repositories for the low resolution dataset for the ndrank worker.

The available repositories are:
- "month-year-repository" (repository that works with data organized by months)
- "hour-day-month-year-repository" (repository that works with data organized by hours (single hour per file))
- "month-year-round-robin-repository" (repository that works with files organized by month that have been organized with a round robin strategy (split mode of the distributor tool))
- "dummy-repository" (does nothing)

All implementations can be found under the folder ```repository/implementations```.

To add a new repository, a new ```.py``` file must be created under that directory.

The class should inherit the ```RepositoryLayer``` class and be decorated with the ```@component_injector.inject_repository(<used tag>)``` decorator, in order to inject the new implementation. The ```dummy.py``` can be used as an example.

#### month-year-repository
Repository that deals with data that is organized by months. Allows the access by timestamp and in a sequential manner. 

#### hour-day-month-year-repository
Repository that deals with data that is organized by hours. Allows the access by timestamp and in a sequential manner. Not used regularly and not recommended

#### month-year-round-robin-repository
Allows the others to data that hes been distributed in a round robin strategy. It only allows access by timestamp, it does not allow an iteration of the dataset one by one.

### settings.yaml
The ```settings.yaml``` file is a file used to get all required information to access the available portion of the dataset. The structure of the settings file is organized as follows:
```
metadata:
  step: <step value>
  time-variation-dim: <name of the variable>
  time-initial-dim: <name of the variable>
  data-vars:
    - <available data var>
    - <available data var>
    ...
  resolution-reduction-parameters:
    <dimension>: <numeric value>
    <dimension>: <numeric value>
    ...
  time-gap:
    <tag to refer time>:
      <numeric value representing the time instance>:
        - <data variable or all>
        - <data variable or all>
        ...
      <numeric value representing the time instance>:
        - <data variable or all>
        - <data variable or all>
settings:
  - <file name>
  - <file name>
  ...
```
##### step
step is the value, in nanoseconds, that represents the resolution of the time dimension (for example 3 hours, 6 hours, 1 hour, etc.). This value has to be indicated, as sometimes it is not possible to directly determine this value from the files existing in the dataset.

##### time-variation-dim and time-initial-dim
The used datesets have the concept of time defined with two different dimensions: 
- A dimension with a single value representing the first date
- A dimension representing the variation of time, owning many values, with a "constant" difference between them 

The reason for this organization is unknown, and to make things worse in some datasets one is called "step" and the other is called "time" and, sometimes, is the other way around. Because of this, the worker node must be explicitly told what is the name of each in order to deal with the dataset below.

The ```time-variation-dim``` has the name of the dimension that represents the variation of time and ```time-initial-dim``` has the name of the dimension that holds the single initial value.

##### data-vars
The ```data-vars``` tag lists all the data variables that exist in that repository.

##### time-gap
There are situations where there is "gaps" in data. For example, there may not be data for 12 PM for a specific data variable or at 6 AM for all data variables (to the point that the timestamp itself may not exist in the time dimension). 

To deal with this, the "time-app" metadata is given. 

The HOUR tag refers to specific hours of all days where there is no data. For now there is no other tag. 

In the sub tags is where the numeric value for the tag is indicate, so for example if we have: 

```
  time-gap:
    hour:
      6:
        ...
```

It will mark that there is no data at 6AM. 

The last information that must be provided, is which data variables do not have the data. This information is defined with a list in each numerical tag. This list must have either the name of the data variables or a name called ALL for situations where there is no data for that time instance for all data variables. So in the following example:

```
  time-gap:
    hour:
      0:
        - "ALL"
      6:
        - "sd"
```

It indicates that there is no data at 0AM for all data variables and there is no data at 6AM for the "sd" (snow depth) data variable.

#### correlation-functions
The system is capable of using different similarity functions. Some of them use parameters like the average values or the standard deviation. The location of these parameters can be passed in the following way:
```
correlation-functions:
  average-path: <path for average parameters>
  standard-deviation-path: <path for standard deviation parameters>
```

The available implementations for similarity functions that exist are the following:
- pcc
- rmsd
- enhanced-pcc (function that calculates the pcc by first subtracting the average and dividing by the standard deviation. May not be fully functional after the implementation of the multiple repositories)

### Other tags
There are still other remaning tags:
```
node-id: <node id>
...
temporary-folder: <path to temporary folder>
```

The ```node-id``` tag is only essencial when the worker node assumes the role of low resolution node. The ```temporary-folder``` is used to create temporary files and it is essencial for all roles.

