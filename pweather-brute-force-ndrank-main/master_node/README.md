# Master node
This is the node responsible for creating requests in the cluster nodes and receive the results, storing them into csv files.

The master node is configured with two different files:
- the "properties.yaml"
- the "requests.yaml"

The "properties.yaml" has the data required about the total dataset, the existing nodes, etc.

The "requests.yaml" has the requests that are wished to be requested.

## General architecture
The master node is made of a package called "node_client" which is responsible for executing the requests to worker nodes. The rest of the code is only used to allow the execution of the master locally and to register the results in local files.

## Execution and Results
To start the system, make sure the worker nodes and the kafka queue are executing. After this execute the master node, providing the properties file and the requests file

## Available flags:
To execute the master run the following command:

```
python3 __main__.py -r <path of the requests file> -p <path of the properties file>
```

The Master node has the following flags
- -r (path of the requests file)
- -p (path of the properties file)

The ```-r``` allows to override the default "requests.yaml" file path. This flag is optional.

The ```-p``` allows to override the default "properties.yaml" file path. This flag is optional.

## Properties.yaml
The properties file has this general structure:
```
results-path: <path of the results path>
dataset_start_date:
  year: <year>
  month: <month>
  day: <day>
  hour: <hour>
dataset_end_date:
  year: <year>
  month: <month>
  day: <day>
  hour: <hour>
node-properties:
  - ip: <ip>
    port: <port>
  - ip: <ip>
    port: <port>
  ...

```
The ```results-path``` indicates where the results should be stored.

### Dataset properties
The following block provides some information that it is necessary for the master node to work. This is its main structure:
```
dataset_start_date:
  year: 1980
  month: 1
  day: 1
  hour: 6
dataset_end_date:
  year: 1980
  month: 3
  day: 31
  hour: 18
```
This block defines the starting date and end date of the time dimension of the whole distributed dataset.

### Existing nodes
The configuration of the existing nodes is done in the following way:
```
node-properties:
  - ip: <IP>
    port: <PORT>
  ...
```
The ```node-properties``` has the node network configurations for either the brute force nodes or the ndrank worker nodes.
Each element in the list represents a different worker node.

### Results configuration
The results are written into a results file. These are the available configurations:
```
results-path: <folder path>
```

The ```results-path``` provides the path of the folder where the results should be written.

## requests.yaml
The "requests.yaml" file has the required configuration for each request. It is structured in the following manner:

```
requests:
  - request-name: <request name>  
    number-of-results: <number of results>
    time-instances: <integer value>
    options:
      data-vars:
        - <data variable>
        - <data variable>
        ...
      correlation-function: <correlation function>
      ts-neighbour-gap: <integer value>
      data-var-selection:
        - <data variable>
        ...
    input-path:
      <data variable>:
        - <input path>
        - <input path>
        ...
      <data variable>: 
        - <input path>
        - <input path>
        ...
      ...
  ...
```

The ```request-name``` is used in order to create an id for the request. 

The ```number-of-results``` tells the system the number of wanted results. In some services this is mandatory, in some it is not but it will cut the list of results the given number. So if all results are wanted it should be commented out.

The ```time-instances``` and the ```input-path``` define the input. The ```time-instances``` states the size of the input in the time dimension. The ```input-path``` provides a list of paths for the files that should be used an input.

The ```input-path``` tag has the path on the local system where the files used for the search requests are located. They are organized by data variable, and each file represents a single time instance. These files can be in a netcdf format or in grib format.

Inside the options block there are 4 different tags that can be used:
- the ```data-vars``` 
- the ```correlation-function```
- the ```ts-neighbour-gap```
- the ```data-var-selection```

The ```data-vars``` is used to state the data variables that are going to be used in the search. For example if the variables sd and sst are desired then they should be listed in the following manner:
```
  ...
    data-vars:
      - sd
      - sst
  ...
```

The ```correlation-function``` is the name of the similarity function that should be used to calculate the similarity value.

The ```ts-neighbour-gap``` is used when it is necessary, in some step of the search process, to execute a search by timestamp, like after the calculation of the heuristic in NDRank mode. The ```ts-neighbour-gap``` indicates the worker that when they are searching in specific timestamps, instead of looking just at the timestamp itself it also looks at the timestamps around it. If, for example, the ```ts-neighbour-gap``` is set to 2 and the timestamp 1980-01-02T06:00:00 is given, it will also search at the timestamps 1980-01-02T03:00:00 and 1980-01-02T09:00:00 (assuming the time dimension is divided in intervals of 3 hours). To not look for the neighobouring timestamps, the value 1 is used.

The ```data-var-selection``` is used for the service that processes a list of candidates by calculating the similarity for just a small number of data variables. The ```data-var-selection``` indicates to the worker nodes which data variables should be used to calculate the candidate.