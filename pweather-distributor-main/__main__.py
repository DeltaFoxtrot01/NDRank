"""
The main purpose of this project is to grab objects from an existing memory system and 
distribute to other memory systems, with a created strategy

Two strategies are made available:
- Round Robin
- Time Interval
"""
import logging
from typing import Dict, List, Optional, Tuple
from typing_extensions import Final
import yaml
from distributor.auxiliar.split_methods import SplitType
from distributor.distributor import distributor
from downloader.downloader import downloader_interface
from downloader.file_downloader import file_downloader
from downloader.gcp_bucket_downloader import gcp_bucket_downloader
from uploader.local_file_uploader import local_file_uploader
from uploader.node import folder_node
from uploader.uploader import mock_uploader, uploader_interface
from disk_mount import mount_disk_operations

logging.basicConfig(level=logging.DEBUG,format='distributor-%(levelname)s:%(message)s')

#---------------------TAGS FROM PROPERTIES.YAML---------------------

#---------------------BASIC PROPERTIES---------------------
DOWNLOADER: Final = "downloader"
UPLOADER: Final = "uploader"
RUN_WITH_SUDO: Final = "run-with-sudo"
TEMP_DESTINATION_FOLDER: Final = "temp-dest-folder"
STRATEGY: Final = "strategy"
METADATA_STRATEGY: Final = "metadata-strategy"
METADATA_ATTRS: Final = "metadata-attrs"
SPLIT: Final = "split"

#---------------------INTERVAL PROPERTIES---------------------
INTERVAL: Final = "interval"
DOWNLOAD_INTERVAL: Final = "download-interval"
START: Final = "start"
END: Final = "end"

#---------------------START POINT PROPERTIES---------------------
START_POINT: Final = "start-point"
START_FROM_FILE: Final = "start-from-file"
FILE_NAME: Final = "file-name"

#---------------------GCP DOWNLOADER PROPERTIES---------------------
GCP_DOWNLOADER_SETTINGS: Final = "gcp-downloader-settings"
BUCKET_NAME: Final = "bucket-name"
ORIGINAL_FOLDERS: Final = "origin-folders"

#---------------------FILE DOWNLOADER PROPERTIES---------------------
FILE_DOWNLOADER_SETTINGS: Final = "file-downloader-settings"
ORIGIN_FILE: Final = "origin-file"

#---------------------DISK UPLOADER PROPERTIES---------------------
DISK_UPLOADER_SETTINGS: Final = "disk-uploader"
NUMBER_OF_DISKS: Final = "number-of-disks"

#---------------------NAMES OF DOWNLOADERS AND UPLOADERS---------------------
BUCKET: Final = "bucket"
FILE: Final = "file"
MOCKUP: Final = "mockup"
DISK: Final = "disk"

#---------------------END OF TAGS FROM PROPERTIES.YAML---------------------

props: Dict = None
disk_folders: List[str] = None #Used for the folders that hold the mounted disks
downloader: downloader_interface = None
uploader: uploader_interface = None

# loads yaml props
with open('properties.yaml', 'r') as f:
    props = yaml.safe_load(f)

strategy: str = props[STRATEGY]
metadata_strat: str = props[METADATA_STRATEGY]

#---------------------CONFIGURE DOWNLOADER---------------------
if props[DOWNLOADER] == FILE:
    options: Dict = props[FILE_DOWNLOADER_SETTINGS]
    downloader = file_downloader(options[ORIGIN_FILE])
elif props[DOWNLOADER] == BUCKET:
    options: Dict = props[GCP_DOWNLOADER_SETTINGS]
    downloader = gcp_bucket_downloader(options[BUCKET_NAME],
                                        options[ORIGINAL_FOLDERS],
                                        props[TEMP_DESTINATION_FOLDER])
else:
    raise ValueError("Invalid downloader type: " + props[DOWNLOADER])


#---------------------CONFIGURE UPLOADER---------------------
if props[UPLOADER] == MOCKUP:
    uploader = mock_uploader(4)
elif props[UPLOADER] == FILE:
    uploader = local_file_uploader()
    uploader.add_nodes(
        [
            folder_node("./dev_dest_file_0"),
            folder_node("./dev_dest_file_1")
            #folder_node("/home/ddmdavid/second_disk/IST/5ano/tese/portions/grib2/portion1_split"),
            #folder_node("/home/ddmdavid/second_disk/IST/5ano/tese/portions/grib2/portion2_split")        
        ]
        #[folder_node("/home/ddmdavid/second_disk/IST/5ano/tese/pweather-brute-force/worker_node/testing_dataset_days")]
    )
elif props[UPLOADER] == DISK:
    """As a simplification the "disk uploader" is implemented by using 
    the local file uploader after the disks have been mounted.
    """
    options: Dict = props[DISK_UPLOADER_SETTINGS]
    num_of_disks: int = options[NUMBER_OF_DISKS]
    temp_folder: str = props[TEMP_DESTINATION_FOLDER]
    
    disk_folders = mount_disk_operations.automated_mount(num_of_disks, temp_folder, props[RUN_WITH_SUDO], props[START_POINT][START_FROM_FILE])
    uploader = local_file_uploader()
    uploader.add_nodes(list(map(lambda folder: folder_node(folder), disk_folders)))
else:
    raise ValueError("Invalid uploader type: " + props[UPLOADER])

#---------------------CONFIGURE INTERVAL OF FILES TO BE DOWNLOADED---------------------
interval: Optional[Tuple[str,str]] = None
if props[INTERVAL][DOWNLOAD_INTERVAL]:
    interval = (props[INTERVAL][START],props[INTERVAL][END])

#---------------------EXECUTE DISTRIBUTION PROCESS---------------------
distributor_instance: distributor = distributor(downloader,uploader,props[TEMP_DESTINATION_FOLDER], props[METADATA_ATTRS])

options: Dict = props[START_POINT]
if props[SPLIT]:
    if options[START_FROM_FILE]:
        distributor_instance.download_split_and_upload(metadata_strat, SplitType.HOUR, interval, options[FILE_NAME])
    else:
        distributor_instance.download_split_and_upload(metadata_strat, SplitType.HOUR, interval)
else:
    if options[START_FROM_FILE]:
        distributor_instance.download_and_upload(strategy, metadata_strat, interval, options[FILE_NAME])
    else:
        distributor_instance.download_and_upload(strategy, metadata_strat, interval)


#---------------------NECESSARY CLEANUP---------------------
if props[UPLOADER] == DISK:
    mount_disk_operations.automated_umount(disk_folders, props[RUN_WITH_SUDO])
