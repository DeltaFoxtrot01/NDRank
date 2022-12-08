# pweather-distributor

The pweather-distributor tool is the tool responsible for reading a dateset from a specific source (like a gcp bucket or a folder) and distribute it by other storage options used by the Worker nodes like disks or folders. 

## General Architecture
The distributor's architecture has 3 main components: 
- The downloader
- The distributor
- The uploader

The distributor is the class respon­sible for managing the whole distribu­tion process and it has two main modes: the normal mode and the split mode. The regular mode simply gets a file from the downloader (class responsible for getting the dataset files), determines in which worker node it should be placed and uses the uploader (class responsible for uploading the files the worker's storage) to upload the file. 

The downloader and the uploader have a base interface. From these interfaces different implementations are made, each injectable via the properties file. 

## Local Environment

### dev_* folders
The dev_* folders are used in a development environment for testing purposes. So instead of immediately testing with buckets and disks, folders can be used to test the tool locally. 

These folders are both used as sources and destinations for development

### Configure Conda environemnt
To create an environment
- conda env create -f conda_env.yaml 

To activate the conda environment run:
- conda activate pweather-distributor

### Setup GCP key file
To operate with objects in the google cloud, a key file is necessary. Place a key file from gcp in the key_files folder, rename it bucket-key.json and run the setup script in the following manner:
- source setup_gcp_access_key.sh

### Run unit tests
To run existing tests, run the following command:
- py.test

The existing tests only cover a really small section of the code. This occured as it was not seen a simple way to develop unit tests that could cover the full tool (or even most of the features). 

Because of this, most of the testing was done by executing the tool and verifying results manually.

## Docker environment
To make it easier to deploy the tool in different environments, the tool was containerized with docker. To run the docker container, make sure both docker and docker compose are installed in the host machine and run:

```
docker-compose up -d
```

To verify the logs that have been produced run:

```
docker-compose logs
```

To follow the logs as they are being produced add the ```-f``` flag at the end of the logs command.

The container is built by GCP build, and it's configuration can be seen in the ```cloudbuild.yaml``` file.

Docker compose builds the following paths as volumes:
- ./log_files
- ./key_files/bucket-key.json
- ./properties.yaml

Both the ```bucket-key.json``` and the ```properties.yaml``` must be externally provided.

## Distributing parameters
To distribute parameters like average or standard deviation, a separate bash script is used. This script is called ```download_parameters.sh```. This tool has 2 modes of execution:
 - mount
 - umount

The "mount" mode gathers the existing disks, attaches them and downloads the parameters to all the disks. It has the following parameters:
```
./script.sh MOUNT <NUMBER OF DISKS> <BUCKET PATHS INCLUDING NAME OF THE BUCKET TO DOWNLOAD>
```
The ```NUMBER OF DISKS``` is the number of existing disks. The ```BUCKET PATHS INCLUDING NAME OF THE BUCKET TO DOWNLOAD``` is where the paths of each folder containing the parameters are placed, each separated by a space.

The "umount" mode umounts disks that have been mounted during the "mount" mode (does not wipe them out). It has the following structure:
```
./script.sh UMOUNT <NUMBER OF DISKS>
```

## properties.yaml file
### Basic structure
This tool is configured with the 'properties.yaml' file.

The 'properties.yaml' has the following general configurations:

```
strategy: <available strategies>
metadata-strategy: <available metadata strategies>
downloader: <downloader type>
uploader: <uploader type>
run-with-sudo: <boolean value>
temp-dest-folder: <folder to be used for temporary files>
split: <split mode>

metadata-strategy: "low-res-netcdf-grib"
metadata-attrs:
  time-variation-dim: <name of the variable>
  time-initial-dim: <name of the variable>
  step: <float value>
  time-gap:
    <tag to refer time>:
      <numeric value representing the time instance>:
        - <data variable or all>
        - <data variable or all>
        ...
      <numeric value representing the time instance>:
        - <data variable or all>
        - <data variable or all>
        ...
      ...
    ...        

  resolution-reduction-parameters:
    <dimension>: <numeric value>
    <dimension>: <numeric value>
    ...

interval:
  download-interval: <boolean value>
  start: <file name>
  end: <file name>

start-point:
  start-from-file: <boolean value>
  file-name: <file name>

gcp-downloader-settings:
  bucket-name: <bucket name>
  origin-folders:
    - <folder path>
    - <folder path>
    ...

file-downloader-settings:
  origin-file: <folder path>

disk-uploader:
  number-of-disks: <number of disks>

```

#### strategy:
The distributor uses different strategis to distribute the files by the existing uploaders (by uploaders, it is meant the storage solutions selected for the worker nodes).

These are the available strategies:
- round-robin-reduced
- round-robin-mock
- round-robin-grib
- time-interval-reduced
- time-interval-mock
- time-interval-grib

The "reduced", "mock" and "grib" refer to the file name format. 
The "reduced" deals with file names with the following format:
```
<YEAR>-<MONTH>-<DAY> <HOUR>:<MINUTE>:<SECOND>
```

The "mock" deals with file names that consist in a integer number. 
The "grib" deals with file names that have the following format:
```
ERA5-<MONTH>-<YEAR>.grib|nc
```

By following these file formats, the tools is able to sort them by the time intervals the files have and distribute them according to that order.


#### metadata-strategy:
When accessing a portion of the dataset, the worker uses a file called "settings.yaml". The "settings.yaml" file is a file that contains all the information required to know all the files that exist in worker's portion of the dataset and some more information about the dateset like the name of the time dimension (for some reason, some datasets call it "step" other call it "time"), the size of the step value, etc. 

This is what is referred as metadata and distributor tool also generates the file containing the metadata of the specific portion of the dataset. This file is also submitted to the storage solution managed by the uploader.

There are two different strategies to generate the metadata and they are named follows: 
- netcdf-grib
- low-res-netcdf-grib

The "netcdf-grib" strategy is used when the distribution is made for the full resolution dataset, or for the section of a brute force worker.

The "low-res-netcdf-grib" is used for low resolution datasets, containing all the information provided by the "netcdf-grib" strategy and other metadata required like 
the resolution reduction parameter.

The metadata attributes can be configured as the following example shows:
```
metadata-attrs:
  time-variation-dim: "time"
  time-initial-dim: "step"
  step: 86400000000000.0
  time-gap:
    hour:
      0:
        - "ALL"
      6:
        - "ALL"

  resolution-reduction-parameters:
    latitude: 2
    longitude: 4
```
##### time-variation-dim and time-initial-dim
The used datesets have the concept of time defined with two different dimensions: 
- A dimension with a single value representing the first date
- A dimension representing the variation of time, owning many values, with a "constant" difference between them 

The reason for this organization is unknown, and to make things worse in some datasets one is called "step" and the other is called "time" and, sometimes, is the other way around. Because of this, the worker node must be explicitly told what is the name of each in order to deal with the dataset below.

The ```time-variation-dim``` has the name of the dimension that represents the variation of time and ```time-initial-dim``` has the name of the dimension that holds the single initial value.

##### step
step is the value, in nanoseconds, that represents the resolution of the time dimension (for example 3 hours, 6 hours, 1 hour, etc.). This value has to be indicated, as sometimes it is not possible to directly determine this value from the files existing in the dataset.

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

##### resolution-reducer-parameters
In the situation where the dataset being distributed is the low resolution dataset, the parameters used to reduce the resolution must be provided, so the worker node knows how to reduce the resolution when an input is received.

To do this, define a list of parameters under the tag ```resolution-reduction-parameters``` in the following way:
```
  <dimension>: <number of times by which the resolution should be reduced>
  ...
```

For example:
```
  latitude: 2
  longitude: 4
```
This example indicates that the ```latitude``` should be reduced by a factor of 2 and the ```longitude``` should be reduced by a factor of 4.

This information is only written into the metadata file if the ```low-res-netcdf-grib``` strategy is used.

#### downloader:
The downloader is a group of clas­ses responsible for obtaining the dataset from some source. The available downloaders are: 
- bucket
- file

#### uploader:
The uploader is a group of classes that place the downloaded files in other storage solutions, like folders or disks. 
- mockup
- file
- disk

#### run-with-sudo:
Certain commands like mount require to use 'sudo'. If set to true will add 'sudo' to those commands.

#### temp-dest-folder:
The temp-dest-folder is used by the tool as a location to store temporary files, like the files downloaded by the downloader. 

### gcp-downloader-settings
General settings for GCP bucket downloader.
```
gcp-downloader-settings:
  bucket-name: <bucket name>
  origin-folders:
    - <folder to look for files>
    ...
```
The bucket uploader is capable of downloading files from multiple sources. This "multiple source" download can only be used in the regular distribution mode, not in in the Split mode (aka ```split: false```)

### file-downloader-settings
General settings for file downloader.
```
file-downloader-settings:
  origin-file: <starting file>
```
It essentially uses a folder as a source for download. Only capable of using one source

### mockup uploader
The mockup uploader is only used for testing. It does not upload anything to anywhere.

### file uploader
The file uploader is only used for testing purposes. It uploads the files into the ```dev_dest_file_0``` folder and the ```dev_dest_file_1```folder. These are hardcoded in the \_\_main\_\_.py file.

### disk-uploader
The dist-uploader detects the existing disks in the instance, mounts them, formats them with a ext4 file system and distributes the portions in a similar manner to the file uploader. 

The method used to determine the mount path of the disks is by taking the given number of disks and generating a /dev/sd_ path, starting by /dev/sdb. It is assumed that /dev/sda is the main disk of the instance being used. the disk uploader is only capable of going to /dev/sdz, limiting it to 26 disks. 

The settings for the disk uploader go as follow:
```
disk-uploader:
  number-of-disks: <number of available disks>
```

### split mode
The split mode acts in a different manner from the regular mode. Instead of grabbing a file from a downloader and giving to the uploader directly, it first splits into different files and distributes either the timestamps or the days the file has by each newly created file in a round robin fashion.

This mode is organized separately from the rest, because it was complicated at the time to fully integrate this mode with the rest of the code, and it was thought that it was wiser implement it separately, even if it would not have as much functionality as the regular mode. Because of this, this mode does not accept the distribution of different datasets at the same time, for example.

To activate the split mode the value ```true``` can be passed after the ```split``` tag. Otherwise, ```false``` can be passed.


### Interval and starting point
In situations where it may not be intended to use the full dataset, a specific interval can be selected, instead of distribution the full dataset. To do this, the ```interval``` block can be used, and it has the following structure:
```
interval:
  download-interval: <boolean value>
  start: <name of the file>
  end: <name of the file>
```
If it is only a section of the dataset is desired, then the ```download-interval``` should be set to ```true```. To define the interval of values the ```start``` and ```end``` tags must have the starting file and the ending file. The order that determines the order of the files depends on the configurations that the tool has been provided. For example, the tool is able to sort files that follow the format ```ERA5-<month>-<year>.grib|nc```, where "month" is the month the file it has and "year" is the year of which the month belongs to.


In a scenario where the tool crashes and it is intended to say where the tool should start, the ```start-point``` block can be used. 
The ```start-point``` has the following structure:
```
start-point:
  start-from-file: <boolean>
  file-name: <name of the file where to start>
```

To start from a specific file, the ```star-from-file``` attribute must be set to true and the file where it should start has to be stated after the ```file-name``` tag.
