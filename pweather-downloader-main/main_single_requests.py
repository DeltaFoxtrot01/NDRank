from os import remove
import os
from downloader_service.cds_downloader import execute_request, execute_single_request
from storage_service.bucket_storage_service import bucket_storage_service
import yaml, threading

ORIGIN_FOLDER = "netcdf-gaivota/"

props = None

with open('dev-single-requests-properties.yaml', 'r') as f:
    props = yaml.safe_load(f)

bucket_client = bucket_storage_service("pweather-penedocapital")                                                                                                                                                                                            


#transfers fragment of dataset to bucket
def handler(arguments):
    filename = ORIGIN_FOLDER + arguments.attribute_type + "/" + arguments.filename
    bucket_client.upload_file(filename, arguments.filename,
                              lambda : print("Successfully transfered " + arguments.filename), 
                              lambda : print("Failed to transfer " + arguments.filename))

#after a successful transfer of the .grib file, do all required processing
def process_grib(filename: str, bucket_folder: str):

    def success_handler(filepath: str):
        print("Successfully transfered ", filepath)
        os.remove(filepath)

    filename_grib = filename.split("/")[-1]
    print(filename_grib)
    bucket_client.upload_file(bucket_folder + "/" + filename_grib, filename,
                              lambda : success_handler(filename), 
                              lambda : print("Failed to transfer " + filename_grib))

def main():
    def create_task(file: str, bucket_folder: str):
        task = threading.Thread(target = lambda : process_grib(file, bucket_folder))
        task.start()
        return task
    
    threads = []

    for request in props["requests"]:
        print(request)
        execute_single_request(request["year"], 
                               request["month"], 
                               props["output"],
                               props["request"],
                               lambda file: threads.append(create_task(file, props["bucket_folder"])),
                               lambda file, reason: print("Failed to transfer file ", file, " for reason: ", reason))
                    
    for thread in threads:
        thread.join()

main()