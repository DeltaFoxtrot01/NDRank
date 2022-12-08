from datetime import datetime
from schema import Schema, Or #type: ignore
from typing import Any, Dict, Final, List, Union
import re

#tags:
GCP_DOWNLOADER_SETTINGS: Final = "gcp-downloader-settings"
BUCKET_NAME: Final = "bucket-name"
ORIGIN_FOLDER: Final = "origin-folder"
DESTINATION_FOLDER: Final = "destination-folder"

RESULTS: Final = "results"
FOLDER_PATH: Final = "folder-path"

DATA_PARAMETERS: Final = "data-parameters"
SUBTRACT_AVERAGE: Final = "subtract-average"
DIVIDE_STANDARD_DEVIATION: Final = "divide-standard-deviation"
AVERAGE_PATH: Final = "average-path"
STANDARD_DEVIATION_PATH: Final = "standard-deviation-path"

PARAMETERS: Final = "parameters"
DATA_VARS: Final = "data-vars"
TIME_VAR_DIM: Final = "time-var-dim"
TIME_INIT_DIM: Final = "time-init-dim"
OTHER_SELECTION_PARAMS: Final = "other-selection-params"

WANTED_DATES: Final = "wanted-dates"


class SettingsResults:
    """Object that treats the information from the properties file
    """
    def __init__(self, obj: Dict) -> None:
        self._bucket_name: str = obj[GCP_DOWNLOADER_SETTINGS][BUCKET_NAME]
        self._origin_folder: str = obj[GCP_DOWNLOADER_SETTINGS][ORIGIN_FOLDER]
        self._destination_folder: str = obj[GCP_DOWNLOADER_SETTINGS][DESTINATION_FOLDER]

        self._result_folder_path: str = obj[RESULTS][FOLDER_PATH]

        self._subtract_average: bool = obj[DATA_PARAMETERS][SUBTRACT_AVERAGE]
        self._divide_standard_deviation: bool = obj[DATA_PARAMETERS][DIVIDE_STANDARD_DEVIATION]
        self._average_path: str = obj[DATA_PARAMETERS][AVERAGE_PATH]
        self._standard_deviation_path: str = obj[DATA_PARAMETERS][STANDARD_DEVIATION_PATH]

        self._data_vars: List[str] = obj[PARAMETERS][DATA_VARS]
        self._time_var_dim: str = obj[PARAMETERS][TIME_VAR_DIM]
        self._time_init_dim: str = obj[PARAMETERS][TIME_INIT_DIM]
        self._other_params: Dict[str, Union[int,float]] = \
            {} if obj[PARAMETERS][OTHER_SELECTION_PARAMS] is None else obj[PARAMETERS][OTHER_SELECTION_PARAMS]

        self._wanted_dates: List[datetime] = []

    def add_wanted_date(self, date: datetime) -> None:
        self._wanted_dates.append(date)

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    @property
    def origin_folder(self) -> str:
        return self._origin_folder

    @property
    def destination_folder(self) -> str:
        return self._destination_folder

    @property
    def result_folder_path(self) -> str:
        return self._result_folder_path

    @property
    def subtract_average(self) -> bool:
        return self._subtract_average

    @property
    def divide_standard_deviation(self) -> bool:
        return self._divide_standard_deviation

    @property
    def average_path(self) -> str:
        return self._average_path
    
    @property
    def standard_deviation_path(self) -> str:
        return self._standard_deviation_path

    @property
    def data_vars(self) -> List[str]:
        return self._data_vars

    @property
    def time_var_dim(self) -> str:
        return self._time_var_dim

    @property
    def time_init_dim(self) -> str:
        return self._time_init_dim

    @property
    def other_params(self) -> Dict[str, Union[int,float]]:
        return self._other_params

    @property
    def wanted_dates(self) -> List[datetime]:
        return self._wanted_dates

    def __repr__(self) -> str:
        res: str = ""
        res += "Bucket Name: " + self._bucket_name
        res += "\nBucket Folder: " + self._origin_folder
        res += "\nDestination Folder: " + self._destination_folder
        res += "\n-----------------------------------------"

        res += "\nVariation Time Dim. Name: " + self._time_var_dim
        res += "\nInitial Time Dim. Name: " + self._time_init_dim
        res += "\nData Variables: " + str(self._data_vars)
        res += "\nOther Selection Parameters: " + str(self._other_params)
        res += "\n-----------------------------------------"

        res += "\nWanted Dates: " + str(self._wanted_dates)
        res += "\n"

        return res

_schema: Schema = Schema(
    {
        GCP_DOWNLOADER_SETTINGS:{
            BUCKET_NAME: str,
            ORIGIN_FOLDER: str,
            DESTINATION_FOLDER: str  
        },
        RESULTS:{
            FOLDER_PATH: str
        },
        DATA_PARAMETERS:{
            SUBTRACT_AVERAGE: bool,
            DIVIDE_STANDARD_DEVIATION: bool,
            AVERAGE_PATH: str,
            STANDARD_DEVIATION_PATH: str
        },
        PARAMETERS: {
            DATA_VARS: [str],
            TIME_VAR_DIM: str,
            TIME_INIT_DIM: str,
            OTHER_SELECTION_PARAMS: Or(
                None,
                {
                    str:Or(int,float)
                }
            )
        },
        WANTED_DATES: [str]
    }
)

date_regex: Final = "[0-9]{1,2}[\/][0-9]{1,2}[\/][0-9]{4}[-][0-9]{1,2}[:][0]{2}"

def process_settings(object_properties: Dict) -> SettingsResults:
    _schema.validate(object_properties)

    res: SettingsResults = SettingsResults(object_properties)
    
    for date in object_properties[WANTED_DATES]:
        aux: Any = re.match(date_regex, date)
        if aux is None:
            raise ValueError("Date: " + date + " has an invalid format. DD/MM/YYYY:HH:00")
        res.add_wanted_date(datetime.strptime(date, "%d/%m/%Y-%H:00"))
    return res