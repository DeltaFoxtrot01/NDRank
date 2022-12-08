# pweather-data-visualizer

This tools helps to visualize the obtained results by generating images of the given sections in time.

## Configure Conda environemnt
To create an environment
- conda env create -f conda_env.yaml 

To activate the conda environment run:
- conda activate pweather-data-visualizer

## Setup GCP key file
To operate with objects in the google cloud, a key file is necessary. Place a key file from gcp in the key_files folder, rename it bucket-key.json and run the setup script in the following manner:

- source setup_gcp_access_key.sh


## Properties file

The properties file has the following structure:
```
#bucket configurations
gcp-downloader-settings:
  bucket-name: <name of the bucket>
  origin-folder: <folder path in bucket>
  destination-folder: <destination path>

#result's configuration
results:
  folder-path: <folder path>

#parameter's paths
data-parameters:
  subtract-average: <boolean value>
  divide-standard-deviation: <boolean value>
  average-path: <path of average parameters>
  standard-deviation-path: <path of standard deviation parameters>
  
#dataset configurations
parameters:
  data-vars:
    - <data variable>
    - <data variable>
    ...
  time-var-dim: <name of the dimension>
  time-init-dim: <name of the dimension>
  other-selection-params:
    <dimension>: value

#dates that should produce 
wanted-dates:
  - <timestamp>
  - <timestamp>
  ...

```

### gcp-downloader-settings
The data visualizer assumes the dataset is stored in gcp bucket and that the files follow the name convension ```ERA5-<month>-<year>.nc|grib```.
The ```bucket-name``` tag indicates the name of the bucket.
The ```origin-folder``` indicates what is the folder in the bucket where the dataset is located.
The ```destination-folder``` indicates the local path where the files should be downloaded to.

### results
The ```results``` just indicates where the images should be downloaded. This is done after the ```folder-path``` tag.

### data-parameters
In certain situations it is necessary to use certain parameters to generate the image. It may be necessary to subtract the average or divide the standard deviation. To subtract the average, set ```subtract-average``` to true and pass the path for the average parameters index file after the tag ```average-path```. To divide by the standard deviation, set ```divide-standard-deviation``` to true and pass the path after the tag ```standard-deviation-path```.

### parameters

The ```parameters``` block indicates what are the names of the time dimensions, what data variables should be selected to generate the images and what other dimensions should be selected before generating an image.

#### data-vars
It is only possible to generate an image for one variable. Therefore, this tool allows the generation of different images, each for a specific data variable. To pass the variables that should be used to generate the images, list them under this block like this:
```
  data-vars:
    - sd
    - sst
```

#### time-variation-dim and time-initial-dim
The used datesets have the concept of time defined with two different dimensions: 
- A dimension with a single value representing the first date
- A dimension representing the variation of time, owning many values, with a "constant" difference between them 

The reason for this organization is unknown, and to make things worse in some datasets one is called "step" and the other is called "time" and, sometimes, is the other way around. Because of this, the worker node must be explicitly told what is the name of each in order to deal with the dataset below.

The ```time-variation-dim``` has the name of the dimension that represents the variation of time and ```time-initial-dim``` has the name of the dimension that holds the single initial value.

#### other-selection-params
In case there is the need to fix the value of certain dimensions (like the "altitude" dimensions), this part can be used. To fix the value of the "isobaricInhPa", pass the following:
```
  other-selection-params:
    isobaricInhPa: 250
```

### wanted-dates
To pass the timestamps to generate the images, pass something similar to the following example:
```
wanted-dates:
  - 12/3/2000-18:00
  - 1/7/1990-12:00
  - 1/7/1990-06:00
  - 1/7/1990-00:00
```
