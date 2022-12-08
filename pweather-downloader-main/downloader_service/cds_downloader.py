# Module responsible for executing downloads in the cds API
import os, cdsapi, sys
from types import FunctionType
from calendar import monthrange

SIZE_IN_BYTES_OF_DOWNLOAD = 600*1024*1024

request_collection = {
    "request1": {
        'class': 'ea',
        'expver': '1',
        'levelist': '250/500/850',
        'levtype': 'pl',
        'param': '129.128/130.128',
        'step': '0/6',
        'stream': 'oper',
        'time': '06:00:00/18:00:00',
        'type': 'fc'
    },
    "request2": {
        'class': 'ea',
        'expver': '1',
        'levtype': 'sfc',
        'param': '31.128/34.128/39.128/40.128/139.128/141.128/151.128/169.128/170.128',
        'step': '0/3/6/9',
        'stream': 'oper',
        'time': '06:00:00/18:00:00',
        'type': 'fc',
    },
    "request3": {
        'class': 'ea',
        'expver': '1',
        'levelist': '5/10/100/250/500/850',
        'levtype': 'pl',
        'param': '130.128/131/132',
        'step': '0/6/12',
        'stream': 'oper',
        'time': '06:00:00/18:00:00',
        'type': 'fc',
    },
    "request4":  {
        'class': 'ea',
        'expver': '1',
        'levelist': '200/500/700/850',
        'levtype': 'pl',
        'param': '60.128/135.128/138.128/155.128',
        'step': '0/6/12',
        'stream': 'oper',
        'time': '06:00:00/18:00:00',
        'type': 'fc',
    },
    "request5": {
        'class': 'ea',
        'expver': '1',
        'levtype': 'sfc',
        'param': '37.235/38.235/39.235/40.235/49.235/50.235',
        'step': '0/6/12',
        'stream': 'oper',
        'time': '06:00:00',
        'type': 'fc',
    },
    "request6":  {
        'class': 'ea',
        'expver': '1',
        'levelist': '10/50/100/200/300/400/600/700/800',
        'levtype': 'pl',
        'param': '129.128/135.128',
        'step': '0/6',
        'stream': 'oper',
        'time': '06:00:00/18:00:00',
        'type': 'fc'
    }
}


    

#Returns the number of days existing in a month
def number_of_days_in_month(year: int, month: int) -> int:
    if(month < 1 or month > 12):
        raise ValueError("month is invalid")
    return monthrange(year, month)[1]

#Given a path for a file, returns the size of the file in bytes
def size_from_filepath(file_path: str):
    return os.path.getsize(file_path)

#Creates the dictionary with the pretended request
def create_request_for_month(year: int, month: int, request_type: str):

    month_str = str(month)
    if month < 10:
        month_str = "0" + month_str
    
    day_aux = number_of_days_in_month(year,month)
    day_str = str(day_aux)
    if day_aux < 10:
        day_str = "0" + day_str

    date_string = str(year)+month_str+"01/to/"+str(year)+month_str+day_str
    #date_string = str(year)+month_str+"01/to/"+str(year)+month_str+"02"

    req = request_collection[request_type]
    req["date"] = date_string


    return req


def execute_request(start_year: int, end_year: int, file_path: str, request_type: str, success_handler: FunctionType, failure_handler: FunctionType):
    """
    Main loop of execution responsible for executing every
    single request.
    Arguments:
        - start_year: beginning year
        - end_year: ending year
        - file_path: starting path where file should be temporarily 
        - success_handler: function to be executed if the request is successful.
        Expects one argument, where the name of the file is going to be placed
        - failure_handler: function to be executed if the request fails. Expects
        two arguments, first with the name of the file, second with the error message

    Does the following steps:
    1) build the request
    2) create the request
    3) verify if the size of the file is normal
    4) executes respective handler
    """
    if(start_year > end_year):
        raise ValueError("start year is bigger than the end year")
    c = cdsapi.Client()

    for year in range(start_year, end_year+1):
        for month in range(1, 13):
            out_filename = file_path + "/ERA5-" + str(month) + "-" + str(year) + ".grib"
            bucket_file_name = "ERA5-" + str(month) + "-" + str(year) + ".grib"
            req = create_request_for_month(year, month, request_type)
            if not os.path.exists(out_filename):
                print(req)
                try:
                    c.retrieve('reanalysis-era5-complete', req, out_filename)
                
                    size_of_file = size_from_filepath(out_filename)

                    print("File size: ", size_of_file)
                    #if SIZE_IN_BYTES_OF_DOWNLOAD *0.9 < size_of_file < 1.1 * SIZE_IN_BYTES_OF_DOWNLOAD:
                    success_handler(out_filename)
                    #else:
                    #    failure_handler(bucket_file_name, "File size was not expected: " + str(size_of_file/(1024**2)))
                
                except Exception as e:
                    print(sys.exc_info())
                    failure_handler(bucket_file_name, str(e))
            else:
                failure_handler(out_filename, "File path not found")


def execute_single_request(year: int, month: int, file_path: str, request_type: str , success_handler: FunctionType, failure_handler: FunctionType):
    """
    Used to execute single requests
    """
    c = cdsapi.Client()
    out_filename = file_path + "/ERA5-" + str(month) + "-" + str(year) + ".grib"
    bucket_file_name = "ERA5-" + str(month) + "-" + str(year) + ".grib"
    req = create_request_for_month(year, month, request_type)
    if not os.path.exists(out_filename):
        print(req)
    try:
        c.retrieve('reanalysis-era5-complete', req, out_filename)
    
        size_of_file = size_from_filepath(out_filename)
        print("File size: ", size_of_file)
        #if SIZE_IN_BYTES_OF_DOWNLOAD *0.9 < size_of_file < 1.1 * SIZE_IN_BYTES_OF_DOWNLOAD:
        success_handler(out_filename)
        #else:
        #    failure_handler(bucket_file_name, "File size was not expected: " + str(size_of_file/(1024**2)))
    
    except Exception as e:
        print(sys.exc_info())
        failure_handler(bucket_file_name, str(e))