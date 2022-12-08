import os, sys
from downloader_service.cds_downloader import execute_request
from conversion_service.from_grib_to_cdf import separate_grib_and_reduce_resolution, separate_grib_in_cdf_files
from storage_service.bucket_storage_service import bucket_storage_service
import yaml

props = None

if len(sys.argv) != 2:
    print("Must pass the name of the properties file")
    sys.exit(1)

with open(sys.argv[1], 'r') as f:
    props = yaml.safe_load(f)


for file in props["files"]:
    print(file)
    separate_grib_and_reduce_resolution(file,[0,12],8)
    os.remove(file)
    print("File transfered with success:", file)