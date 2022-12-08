import os
from time import time
from types import FunctionType
from typing import List
import xarray as xray
import numpy as np
import pandas as pd
from storage_service.bucket_storage_service import bucket_storage_service


PARAM_MAPPING_DCT = {'t2m': '2t',
                     'u100': '100u',
                     'v100': '100v'}

LATITUDE = "latitude"
LONGITUDE = "longitude"
TIME = "time"

class fragment_attributes:
    '''
    Datastructure storing all information related to the 
    created fragment of the dataset
    '''
    def __init__(self, filename: str = None, attribute_type: str = None):
        self._filename = filename
        self._attribute_type = attribute_type
    
    @property
    def filename(self):
        return self._filename

    @filename.setter
    def filename(self, value: str):
        self._filename = value

    @property
    def attribute_type(self):
        return self._attribute_type

    @attribute_type.setter
    def attribute_type(self, value: str):
        self._attribute_type = value


def separate_grib_in_cdf_files(file_path: str, handler: FunctionType):
    '''
    Receives a .grib file and fragments it into smaller netCDF files
    '''
    ds = xray.open_dataset(file_path, engine="cfgrib", 
                             backend_kwargs={'filter_by_keys': {'shortName': PARAM_MAPPING_DCT['t2m']}})

    size = ds.coords["time"].size
    print("Num of datasets: ", str(size))
    nr = 1
    for by_time in ds.coords["time"]:
        aux_set = ds.interp(time=np.datetime64(by_time.values))

        file_name = str(np.datetime64(by_time.values)) + ".nc"

        args = fragment_attributes(file_name,"t2m")

        aux_set.to_netcdf(file_name)
        print("File created: " + file_name, "\tnr " + str(nr) + " out of " + str(size))
        handler(args)

        os.remove(file_name)
        nr += 1


def separate_grib_and_reduce_resolution(file_path: str, desired_hours: list, degree_reduction_factor: int):

    for tag in PARAM_MAPPING_DCT:
        ds = xray.open_dataset(file_path, engine="cfgrib", 
                             backend_kwargs={'filter_by_keys': {'shortName': PARAM_MAPPING_DCT[tag]}})

        bucket_client = bucket_storage_service("pweather-penedocapital")

        size = ds.coords[TIME]
        print(size)

        for by_time in ds.coords[TIME]:
            date_pandas = pd.to_datetime(by_time.values)
            #filter timestamps
            if date_pandas.hour in desired_hours:
                aux_da = ds.interp(time=np.datetime64(by_time.values))[tag]

                aux_da =aux_da.coarsen(latitude=degree_reduction_factor, boundary="trim")\
                                    .mean()\
                                    .coarsen(longitude=degree_reduction_factor, boundary="trim")\
                                    .mean()

                file_path_temp = "./temp/" + str(date_pandas) + ".nc"
                print("Going to upload file: ", file_path_temp)
                aux_da.to_netcdf(file_path_temp)
                bucket_client.upload_file("netcdf-reduced/" + tag + "/" + str(date_pandas), file_path_temp, lambda : print("Successfully transfered ", file_path_temp), lambda : print("Failed to transfer ", file_path_temp))
                os.remove(file_path_temp)

        ds.close()