# pweather-resolution-reducer

Process responsible for processing weather dataset for the following purposes:
- Resolution reduction
- Conversion from one type of grid to another

Is capable of working with multiple sources such as folders from the local file system to google cloud buckets.

## General Architecture
The resolution reducer has 3 main components:
- The downloader
- The main_process_execution_engine
- The uploader

The main_process_execution_engine is the main class responsible for tool's execution and it has two modes: the "split-by-vars" and the "filter-files". 

The "filter-files" is the main execution mode and it allows the processing of grib and netcdf files with the usage of a routine that implements a "pipe and filter" to process each file with a different filters. The name given to these filters is "reducer". It has the name "reducer", because the tool was originally intended to only reduce the resolution of datasets. However, it was realized that the architecture that the tool had allowed it to do more than that, and at this moment "reducers" are not only responsible for reducing the resolution of datasets, but are also responsible for converting between different types of grids, remove sections that are just filled with 0s, etc.

The "split-by-vars" only does one thing: get a file, read it's data variables and create separate files for every data variable. 

The downloader is used to download the files of a dataset from a specific source and the uploader uploads these files (after being processed) into another storage solution. The downloader and the uploader have a base interface. From these interfaces different implementations are made, each injectable via the properties file.

## Local Environment

## dev_* folders
The dev_* folders are used in a development environment for testing purposes. So instead of immediately testing with buckets and disks, folders can be used to test the tool locally. 

These folders are both used as sources and destinations for development

### Configure Conda environemnt
To create an environment
- conda env create -f conda_env.yaml 

To activate the conda environment run:
- conda activate pweather-resolution-reducer

### Setup GCP key file
To operate with objects in the google cloud, a key file is necessary. Place a key file from gcp in the key_files folder, rename it bucket-key.json and run the setup script in the following manner:
- source setup_gcp_access_key.sh

### Unit Tests
Currently there are no unit tests in this repository, as developing unit tests with large .grib files is not a trivial process.

Because of this, most of the testing was done by executing the tool and verifying results manually.

## Docker environment
This tool is fully containerized. Docker and Docker compose are used to make deployments easier. The configurations for a CI/CD pipeline are also available in the ```cloudbuild.yaml```, this file only builds and pushes the docker container into google cloud container artifact.

Docker compose builds the following paths as volumes:
- ./log_files
- ./key_files/bucket-key.json
- ./properties.yaml

Both the ```bucket-key.json``` and the ```properties.yaml``` must be externally provided.

To start docker compose run the following command:
- docker-compose up -d

This will run the process in the background. To access the produced output run the following command:
- docker-compose logs

Add the ```-f``` to follow the logs in real time.

All other logs that are written in the ```log_files```.

The container is built by GCP build, and it's configuration can be seen in the ```cloudbuild.yaml``` file.

## properties.yaml file
### Basic structure
The current software artifact is configured with the 'properties.yaml' file.

The first section has general configurations:
```
downloader: <downloader type>
uploader: <uploader type>
download-past-future: <boolean value>
reducer-strategies:
  - <reducer name>
  - <reducer name>
  ...

temp-dest-folder: <temporary folder to store the files>
reduction-factor: <integer value>
resulting-average-folder: <path for average folder>
resulting-standard-deviation-folder: <path for standard deviation>
interval: <integer value>
time-first-dim: <variable name>
time-variation-dim: <variable name>

dimensions-to-be-reduced:
  <dimension name>: <integer value>
  <dimension name>: <integer value>

operation-type: split-by-vars|filter-files
files-to-download:
  mode: all|interval|specific|months
  interval:
    start: <file name>
    end: <file name>
  specific:
    - <file name>
    - <file name>
    ...
  months:
    - <integer value>
    - <integer value>
    ...

gcp-downloader-settings:
  bucket-name: <bucket name>
  origin-folder: <path of the folder>

gcp-uploader-settings:
  bucket-name: <bucket name>
  destination-folder: <path of the folder>

file-downloader-settings:
  origin-file: <folder path>

file-uploader-settings:
  dest-file: <folder path>

```

#### downloader:
Downloader source used. The ones available are:
- file
- bucket

The 'file' downloader works directly with the file sytem, and uses a source a folder. The 'bucket' downloader works with a gcp bucket.

#### uploader:
Source to upload the created files. The available ones are:
- file
- bucket

The 'file' uploader works directly with the file sytem, and uses a destination a folder. The 'bucket' uploader works with a gcp bucket.

#### download-past-and-future
In some situations, it is necessary to have not only the file itself, but also the files that, in the perspective of the time dimension, that represent the "past" and the "future" of the file in question. For example, if we are processing the file "ERA5-1-1990.nc", it may be useful to have the file "ERA5-12-1989.nc" and the file "ERA5-2-1990.nc".

To enable the download of the past and future files, the ```download-past-and-future``` must be set to true.

#### reducer-strategy:
The reducer-strategy refers to the name of the reducers that can be applied to each file. There are all available strategies:
- grib_surface_resolution #DEPRECATED
- grib_depth_resolution   #DEPRECATED
- grib_sp_to_grid
- grib_reduced_gaussian_to_grid
- dimension-reducer
- nc-average
- nc-standard-deviation
- anomaly-reducer
- zero-reducer
- netcdf-compressor

To pass the reducers to the pipeline, the only thing that is necessary is to pass them as a list in the ```reducer-strategies```. For example:
```
reducer-strategies:
  - grib_sp_to_grid
  - dimension-reducer
  - netcdf-compressor
```
The reducers will be executed by the order provided in the list.

Both the "grib_surface_resolution" and the "grib_depth_resolution" reduce the the resolution of the dataset by "reduction-factor" times. The "grib_surface_resolution" works with regular gaussian grids that represent surface data and "grib_depth_resolution" works with regular gaussian grids (as well) that represent atmospheric data that are split into multiple pressure levels. To configure by how much should the resolution be reduced is after the tag ```reduction-factor```. BOTH OF THESE ARE DEPRECATED AND SHOULD NOT BE USED

The "grib_sp_to_grid" and "grib_reduced_gaussian_to_grid" convert from other existing grids to a regular gaussian grid. The "grib_sp_to_grid" convert from a grid represented in spherical harmonics to a regular gaussian grid and the "grib_reduced_gaussian_to_grid" convert from a reduced gaussian grid to a regular gaussian grid.

The "dimension-reducer" reduces the resolution according to a given set of coordinates under the block "dimensions-to-be-reduced". The reducer will verify if the coordinates are available in the downloaded files and reduce the resolution with the "mean" function. Here is an example:
```
...
dimensions-to-be-reduced:
  latitude: 2
  longitude: 4
...
```
For this set of options, the latitude and longitude dimensions will be reduced by a factor of 2 and 4, respectively.

The "nc-average" and the "anomaly-reducer" are responsible for the creation of anomaly datasets. 

The "nc-average" creates a group of files that store the average of each parameter and position of all years. These files are stored in the file referred in the parameter ```resulting-average-folder``` in the properties file. The uploader must be set to ```none```.
In a similar way the "nc-standard-deviation" is similar to the "nc-average" reducer, but instead of storing the average, it stores the standard deviation. The "nc-standard-deviation" reducer requires that the average is already calculated. The "nc-average" stores the average files in the path given after the tag ```resulting-average-folder``` and the "nc-standard-deviation" stores the results in the path given after the tag ```resulting-standard-deviation-folder```. The "nc-standard-deviation reads the path from the ```resulting-average-folder``` path. Both the "nc-average" and the "nc-standard-deviation" create a yaml file that can act as an index and has the following structure:
```
<month>:
  <day>:
    <hour>:
      - <file path>
      - <file path>
      ...
    ...
  ...
...
```
Both the ```month```, ```day``` and ```hour``` parameter consist in a integer value. This file allows the indexation of files by timestamp.

The "anomaly-reducer" uses the files created by "nc-average" and creates a new dataset only with the anomaly value of the parameters. It seeks for the files created by "nc-average" in the folder reffered in the parameter ```resulting-average-folder```.

In order to create a full anomaly dataset, a execution with "nc-average" must be executed and then the "anomaly-reducer" can be executed. These can not be executed at the same time (to put it other words, they can't placed as different steps of the same pipeline, they must be executed in separate executions of the tool).

The "zero-reducer" simply reads a file by it's time dimensions and removes any grid that may be composed entirely by zeros.

The "netcdf-compressor" grabs a file stored in a netcdf format and reorganizes the file so it can be stored more efficiently, resulting in a smaller file. This is used, because when xarray creates a netcdf file it tends to be much larger than it has to be. This reducer allows the reduction of the size of these files.

All these strategies assume that the source files are in grib or netCDF format and the resolution reducer strategies output a netCDF file (this occurs as xarray is used to reduce the resolution and it only provides support to create netCDF files).

#### temp-dest-folder
The temp-dest-folder is used by the tool as a location to store temporary files, like the files downloaded by the downloader or the files created by the reducers in intermediate processes.

#### interval
In certain reducers like the "nc-average" reducer, it is usefull not to calculate the average of every timestamps with the timestamp alone. In order to reduce any noise that may exist in the data, it may be useful to calculate the average with the day before and the next day as well. To define what is the interval of timestamps the average calculation should take into consideration, the ```interval``` tag can be used. It receives an integer that represents the number of timestamps in each side that should be considered for the calculation.

#### time-variation-dim and time-initial-dim
The used datesets have the concept of time defined with two different dimensions:

A dimension with a single value representing the first date
A dimension representing the variation of time, owning many values, with a "constant" difference between them
The reason for this organization is unknown, and to make things worse in some datasets one is called "step" and the other is called "time" and, sometimes, is the other way around. Because of this, the worker node must be explicitly told what is the name of each in order to deal with the dataset below.

The time-variation-dim has the name of the dimension that represents the variation of time and time-initial-dim has the name of the dimension that holds the single initial value.

#### operation-type
As stated before, the resolution-reducer has two execution modes: the "split-by-vars" and the "filter-files". The "filter-files" is standard mode that uses the reducers to process the dataset files.

The "split-by-vars" grabs the file and splits it into multiple files, each possessing a single data variable. This was developed, because it is useful to execute searches on diffent variables coming from different datasets.

To activate this mode, the name of the operation must be provided after the ```operation-type```.


#### files-to-download:
Settings on which files should be downloaded and processed. It has the following structure:
```
files-to-download:
  mode: all|interval|specific|months
  interval:
    start: <starting file>
    end: <ending file>
  specific:
    <list of files to be downloaded>
  months:
    - <integer value>
    - <integer value>
    ...
```

The 'mode' tag reffers to how the files should be downloaded. The available modes are:
- all (downloads all the files it can find)
- interval (downloads from a specific file to another)
- specific (downloads only a set of files that are explicitly mentioned)
- months (downloads only the files belonging to specific months)

For the 'interval' mode, to define the interval, it is used the 'start' and 'end' tags under the 'interval' tag. All files will be downloaded from the 'start' file to the 'end' file (including the 'end' file itself). This interval is defined by the order in which the ```downloader_interface``` returns.

For the 'specific' mode, to define the files that should be downloaded, it is only necessary to provide a list of the file names under the 'specific' tag.

For the 'months' mode, it is only necessary to pass a list of months under the ```months``` tag.

#### gcp-downloader-settings:
General details of the bucket downloader configuration. It has the following format:

```
gcp-downloader-settings:
  bucket-name: <name of the bucket>
  origin-folder: <path where the files should be looked at>
```

#### gcp-uploader-settings:
General details of the bucket uploader configuration. It has the following format:

```
gcp-uploader-settings:
  bucket-name: <name of the bucket>
  destination-folder: <path where the files should be looked at>
```

#### file-downloader-settings:
General details of the file downloader settings. It has the following structure:

```
file-downloader-settings:
  origin-file: <path to the source folder>
```

#### file-uploader-settings:
General details of the file uploader settings. It has the following structure:

```
file-uploader-settings:
  dest-file: <path to the destination folder>
```