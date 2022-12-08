"""Tool used to extract specific time periods in order to be used as
input files

Raises:
    ValueError: If given parameters are invalid
"""
from typing import Union
import getopt
import sys
from typing import Final, Optional
import numpy as np
import numpy.typing as npt
import xarray

VARIATION_TIME_VAR: Final = "step"
INIT_TIME_VAR: Final = "time"
#NOTE: in certain dataset, the names must be switched.
#this causes a bug in the extraction of the timestamp

def convert_to_two_digit_string(value: int) -> str:
    """Converts an integer to a string with a minimum of 2 digits

    Args:
        value (int): value to be converted

    Returns:
        str: resulting string
    """
    return str(value) if value // 10 > 0 else '0' + str(value)

HELP_STR: Final = \
"""
Tool used to extract specific time periods of a dataset and create separate files.

Options:
-h -> help: shows this menu
-y -> year: year to extract
-m -> month: month to extract
-d -> day: day to extract
-t -> hour: hour to extract
-i -> number of time instances: number of time instances that it is wished to extract
-f -> input file: file to be further analyzed
-o -> folder to put the output
"""

year: Optional[int] = None
month: Optional[int] = None
day: Optional[int] = None
hour: Optional[int] = None
number_of_time_instances: Optional[int] = None
input_file: Optional[str] = None
output_path: Optional[str] = None

opts, args = getopt.getopt(sys.argv[1:],"y:m:d:t:i:f:o:h")
for opt in opts:
    if opt[0] in ("-h"):
        print(HELP_STR)
        sys.exit(0)
    elif opt[0] in ("-y"):
        year = int(opt[1])
    elif opt[0] in ("-m"):
        month = int(opt[1])
    elif opt[0] in ("-d"):
        day = int(opt[1])
    elif opt[0] in ("-t"):
        hour = int(opt[1])
    elif opt[0] in ("-i"):
        number_of_time_instances = int(opt[1])
    elif opt[0] in ("-f"):
        input_file = opt[1]
    elif opt[0] in ("-o"):
        output_path = opt[1] if opt[1][-1] == "/" else opt[1] + "/"
    else:
        print("Flag " + opt[0] + " is not defined")
        sys.exit(0)


if  year is None or \
    month is None or \
    day is None or \
    hour is None or \
    number_of_time_instances is None or \
    input_file is None or \
    output_path is None:
    raise ValueError("All methods must be defined")

ds: xarray.Dataset = xarray.open_dataset(input_file)

year_str: str = convert_to_two_digit_string(year)
month_str: str = convert_to_two_digit_string(month)
day_str: str = convert_to_two_digit_string(day)
hour_str: str = convert_to_two_digit_string(hour)

input_date_str: str = year_str + "-" + \
                      month_str + "-" + \
                      day_str + "T" + \
                      hour_str + ":00:00.000000000"

#first step, verify if the year and month are valid
first_date: Union[np.datetime64, npt.NDArray[np.datetime64]] = ds.coords[INIT_TIME_VAR].values + ds.coords[VARIATION_TIME_VAR].values[0]
last_date: np.datetime64 = ds.coords[INIT_TIME_VAR].values + ds.coords[VARIATION_TIME_VAR].values[-1]
input_dt64: np.datetime64 = np.datetime64(input_date_str)

if not (first_date <= input_dt64 and input_dt64 <= last_date):
    raise ValueError("Atributes provided in terms of year and month do not exist in the given file")

step_values: npt.NDArray[np.timedelta64] = ds.coords[VARIATION_TIME_VAR].values
index: int
found_time_instance: bool = False

for index in range(len(step_values)):
    step: np.timedelta64 = step_values[index]
    if step == input_dt64 - first_date:
        found_time_instance = True
        break

if not found_time_instance:
    raise ValueError("Unable to find time instance")
    
if index + number_of_time_instances > len(step_values):
    raise ValueError("Given file does not contain all requested time instances")

required_section: np.ndarray = step_values[index: index + number_of_time_instances]
print(required_section)

for section in required_section:
    params = {}
    params[VARIATION_TIME_VAR] = section
    ds_section: xarray.Dataset = ds.sel(params)
    file_name: str = str(section + first_date) + ".nc"
    print("Creating file " + file_name)
    ds_section.to_netcdf(output_path + file_name)

ds.close()