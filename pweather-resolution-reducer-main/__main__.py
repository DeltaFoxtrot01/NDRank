import logging
from typing import Dict, List, Optional
from typing_extensions import Final
import yaml
from downloader.file_downloader import file_downloader
from downloader.gcp_bucket_downloader import gcp_bucket_downloader
from resolution_reducer_tools.dimension_reducer import dimension_reducer
from resolution_reducer_tools.grib_depth_reducer import grib_depth_reducer
from resolution_reducer_tools.grib_surface_reducer import grib_surface_reducer
from resolution_reducer_tools.grib_sp_to_gp_reducer import grib_sp_to_gp_reducer
from resolution_reducer_tools.netcdf_compressor import NetcdfCompressor
from resolution_reducer_tools.time_filter import CleanZeroSections
from resolution_reducer_tools.year_average_reducer import anomaly_reducer, year_average_reducer, year_standard_deviation_reducer
from uploader.file_uploader import file_uploader
from uploader.gcp_bucket_uploader import gcp_bucket_uploader
from uploader.uploader import none_uploader, uploader_interface
from downloader.downloader import downloader_interface
from resolution_reducer_tools.resolution_reducer import resolution_reducer_interface
from resolution_reducer_tools.grib_reduced_gaussian_to_gp_reducer import grib_reduced_gaussian_to_gp_reducer
from main_process_execution_engine.main_process_execution_engine import MONTH_SPECIFIC, main_process_execution_engine

logging.basicConfig(level=logging.DEBUG,format='reducer-%(levelname)s:%(message)s')

#---------------------TAGS FROM PROPERTIES.YAML---------------------

#---------------------BASIC PROPERTIES---------------------
DOWNLOADER: Final = "downloader"
UPLOADER: Final = "uploader"
REDUCER_STRATEGIES: Final = "reducer-strategies"
TEMP_DESTINATION_FOLDER: Final = "temp-dest-folder"
REDUCTION_FACTOR: Final = "reduction-factor"
DIMENSION_REDUCED: Final = "dimensions-to-be-reduced"
DOWNLOAD_PAST_FUTURE: Final = "download-past-future"

#---------------------START POINT PROPERTIES---------------------
START_POINT: Final = "start-point"
START_FROM_FILE: Final = "start-from-file"
FILE_NAME: Final = "file-name"

#---------------------GCP DOWNLOADER & UPLOADER PROPERTIES---------------------
GCP_DOWNLOADER_SETTINGS: Final = "gcp-downloader-settings"
GCP_UPLOADER_SETTINGS: Final = "gcp-uploader-settings"
BUCKET_NAME: Final = "bucket-name"
ORIGINAL_FOLDER: Final = "origin-folder"
DESTINATION_FOLDER: Final = "destination-folder"
RESULTING_AVERAGE_FOLDER: Final = "resulting-average-folder"
RESULTING_STANDARD_DEVIATION_FOLDER: Final = "resulting-standard-deviation-folder"
INTERVAL: Final = "interval"

#---------------------FILE DOWNLOADER PROPERTIES---------------------
FILE_DOWNLOADER_SETTINGS: Final = "file-downloader-settings"
ORIGIN_FILE: Final = "origin-file"

#---------------------FILE UPLOADER PROPERTIES---------------------
FILE_UPLOADER_SETTINGS: Final = "file-uploader-settings"
DESTINATION_FILE: Final = "dest-file"

#---------------------NAMES OF DOWNLOADERS AND UPLOADERS---------------------
BUCKET: Final = "bucket"
FILE: Final = "file"
MOCKUP: Final = "mockup"
DISK: Final = "disk"
NONE: Final = "none"

#---------------------REDUCER STRATEGIES---------------------
GRIB_SURFACE_RESOLUTION: Final = "grib_surface_resolution"
GRIB_DEPTH_RESOLUTION: Final = "grib_depth_resolution"
GRIB_SP_TO_GRID: Final = "grib_sp_to_grid"
GRIB_REDUCED_GAUSSIAN_TO_GRID: Final = "grib_reduced_gaussian_to_grid"
NC_AVERAGE: Final = "nc-average"
NC_STANDARD_DEVIATION: Final = "nc-standard-deviation"
ANOMALY_REDUCER: Final = "anomaly-reducer"
DIMENSION_REDUCER: Final = "dimension-reducer"
ZERO_REDUCER: Final = "zero-reducer"
NETCDF_COMPRESSOR: Final = "netcdf-compressor"

#---------------------TIME DIMENSIONS---------------------
TIME_FIRST_DIM: Final = "time-first-dim"
TIME_VARIATION_DIM: Final = "time-variation-dim"

#---------------------FILES TO DOWNLOAD---------------------
FILES_TO_DOWNLOAD: Final = "files-to-download"
FILE_DOWNLOAD_MODE: Final = "mode"
FILE_ALL: Final = "all"
FILE_INTERVAL: Final = "interval"
FILE_SPECIFIC: Final = "specific"
FILE_INTERVAL_START: Final = "start"
FILE_INTERVAL_END: Final = "end"
OPERATION_TYPE: Final = "operation-type"

#---------------------END OF TAGS FROM PROPERTIES.YAML---------------------

#REQUIRED OBJECTS TO CONFIGURE THE main_process_execution_engine
props: Dict
used_downloader: downloader_interface
used_uploader: uploader_interface
reducer_dictionary: Dict[str,resolution_reducer_interface] = {}
options: Dict
# loads yaml props
with open('properties.yaml', 'r') as f:
    props = yaml.safe_load(f)

#---------------------CONFIGURE DOWNLOADER---------------------
if props[DOWNLOADER] == FILE:
    options = props[FILE_DOWNLOADER_SETTINGS]
    used_downloader = file_downloader(options[ORIGIN_FILE])
elif props[DOWNLOADER] == BUCKET:
    options = props[GCP_DOWNLOADER_SETTINGS]
    used_downloader = gcp_bucket_downloader(options[BUCKET_NAME],
                                            options[ORIGINAL_FOLDER],
                                            props[TEMP_DESTINATION_FOLDER])
else:
    raise ValueError("Invalid downloader type: " + props[DOWNLOADER])

#---------------------CONFIGURE REDUCER STRATEGY---------------------
#BUILD THE REDUCER DICTIONARY
reducer_dictionary[GRIB_SP_TO_GRID] = \
    grib_sp_to_gp_reducer(props[TEMP_DESTINATION_FOLDER])

reducer_dictionary[GRIB_SURFACE_RESOLUTION] = \
    grib_surface_reducer(props[TEMP_DESTINATION_FOLDER],props[REDUCTION_FACTOR])

reducer_dictionary[GRIB_REDUCED_GAUSSIAN_TO_GRID] = \
    grib_reduced_gaussian_to_gp_reducer(props[TEMP_DESTINATION_FOLDER])

reducer_dictionary[GRIB_DEPTH_RESOLUTION] = \
    grib_depth_reducer(props[TEMP_DESTINATION_FOLDER],props[REDUCTION_FACTOR])

reducer_dictionary[NC_AVERAGE] = \
    year_average_reducer(props[TEMP_DESTINATION_FOLDER], props[RESULTING_AVERAGE_FOLDER],
                         props[TIME_VARIATION_DIM], props[TIME_FIRST_DIM], props[INTERVAL])

reducer_dictionary[NC_STANDARD_DEVIATION] = \
    year_standard_deviation_reducer(props[TEMP_DESTINATION_FOLDER], 
                         props[RESULTING_STANDARD_DEVIATION_FOLDER], props[RESULTING_AVERAGE_FOLDER],
                         props[TIME_VARIATION_DIM], props[TIME_FIRST_DIM],props[INTERVAL])

reducer_dictionary[ANOMALY_REDUCER] = \
    anomaly_reducer(props[TEMP_DESTINATION_FOLDER], props[RESULTING_AVERAGE_FOLDER],
                         props[TIME_VARIATION_DIM], props[TIME_FIRST_DIM])

reducer_dictionary[DIMENSION_REDUCER] = \
    dimension_reducer(props[TEMP_DESTINATION_FOLDER], props[DIMENSION_REDUCED])

reducer_dictionary[ZERO_REDUCER] = \
    CleanZeroSections(props[TEMP_DESTINATION_FOLDER], props[TIME_VARIATION_DIM], props[TIME_FIRST_DIM])

reducer_dictionary[NETCDF_COMPRESSOR] = \
    NetcdfCompressor(props[TEMP_DESTINATION_FOLDER])

#raise ValueError("Invalid reducer strategy type: " + props[REDUCER_STRATEGIES])

#---------------------CONFIGURE UPLOADER---------------------
if props[UPLOADER] == FILE:
    options = props[FILE_UPLOADER_SETTINGS]
    used_uploader = file_uploader(options[DESTINATION_FILE])
elif props[UPLOADER] == BUCKET:
    options = props[GCP_UPLOADER_SETTINGS]
    used_uploader = gcp_bucket_uploader(options[BUCKET_NAME], options[DESTINATION_FOLDER])
elif props[UPLOADER] == NONE:
    used_uploader = none_uploader()

#---------------------CREATE MAIN DISTRIBUTION INSTANCE---------------------

execution_engine: main_process_execution_engine = \
    main_process_execution_engine(used_downloader,used_uploader,props[TEMP_DESTINATION_FOLDER])

#BUILD RESOLUTION PIPELINE
if not isinstance(props[REDUCER_STRATEGIES], list):
    raise TypeError(REDUCER_STRATEGIES + " must be a list")

list_of_strategies: List[str] = props[REDUCER_STRATEGIES]
for reducer_strategy in list_of_strategies:
    try:
        execution_engine.add_reducer(reducer_dictionary[reducer_strategy])
    except KeyError:
        raise ValueError("Strategy " + reducer_strategy + " does not exist")

#---------------------CONFIGURE MODE OF WHAT FILES SHOULD BE PROCESSED---------------------
start_file: Optional[str] = None
end_file: Optional[str] = None
specific_files: Optional[List[str]] = None
months: Optional[List[int]] = None
mode: str
operation_type: str

options = props[FILES_TO_DOWNLOAD]
mode = options[FILE_DOWNLOAD_MODE]
operation_type = props[OPERATION_TYPE]
if mode == FILE_ALL:
    pass
elif mode == FILE_INTERVAL:
    start_file = options[FILE_INTERVAL][FILE_INTERVAL_START]
    end_file = options[FILE_INTERVAL][FILE_INTERVAL_END]
elif mode == FILE_SPECIFIC:
    specific_files = options[FILE_SPECIFIC]
elif mode == MONTH_SPECIFIC:
    months = options[MONTH_SPECIFIC]
else:
    raise ValueError("Invalid file download type")

#---------------------EXECUTE RESOLUTION REDUCER PROCESS---------------------

execution_engine.execute(mode,operation_type,start_file,end_file, props[DOWNLOAD_PAST_FUTURE], specific_files, months)