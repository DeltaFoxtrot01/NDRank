import logging
import os
import subprocess
from typing import Iterable, List, Tuple, Union
from typing_extensions import Final
from google.cloud.storage.blob import Blob

from google.cloud.storage.bucket import Bucket
from downloader.downloader import downloader_interface
from google.cloud import storage
from google.api_core.exceptions import NotFound

class gcp_bucket_downloader(downloader_interface):
    """
    GCP bucket downloader. Receives a bucket and a group of paths
    and downloads the files from those paths
    """

    def __init__(self, bucket_name: str, source_folders: List[str], temp_dest_folder:str) -> None:
        """
        Constructor for GCP bucket downloader. Sets up the connection 
        to the bucket

        Args:
            bucket_name (str): name of the existing bucket
            source_folders (List[str]): folder where files should be looked for
            temp_dest_folder (str): temporary destination folder for files
        """
        self._client: storage.Client = storage.Client()
        self._bucket_name: str = bucket_name
        self._source_folders: List[str] = source_folders
        self._dest_temp_folder: str = temp_dest_folder
        logging.debug("Given sources: " + str(source_folders))
        if len(source_folders) == 0:
            raise ValueError("At least one source file has to be passed")
        for i in range(len(source_folders)):
            if self._source_folders[i][-1] != "/":
                self._source_folders[i] += "/"
        
        self._bucket: Bucket = self._client.get_bucket(self._bucket_name)

    def get_object_list(self) -> List[str]:
        """
        Lists all existing objects inside the bucket. In case more than
        one path has been provided, the lists are compared to verify if
        all the file names match with one another

        Raises:
            ValueError: if files listed from different sources
            do not match

        Returns:
            List[str]: list of the name of all existing objects
        """
        res: List[str] = []

        for source_folder in self._source_folders:
            iterator: Iterable = self._client.list_blobs(bucket_or_name=self._bucket_name,
                                                         prefix=source_folder)
            current_list: List[str] = []
            
            for object in iterator:                
                file_name: str = object.name
                file_name = file_name.replace(source_folder,"",1)

                if file_name != "":
                    current_list.append(file_name)
            if len(res) == 0:
                res = current_list
            elif set(res) != set(current_list):
                raise ValueError("Lists from both folders do not match")

        return res

    def get_object(self, object_name: str) -> Union[str,List[Tuple[str,str]]]:
        """
        Downloads an object from a bucket 

        Args:
            object_name (str): name of the object to be fetched

        Returns:
            Union[str,List[Tuple[str,str]]]: path to the downloaded 
            object or multiple objects with the first attribute as 
            the path of the file and the second as a distinguishing 
            name (like the name of a data variable) 
        """
        try:
            if len(self._source_folders) == 1:
                origin_folder: str = self._source_folders[0] + object_name
                destination_file: str = self._dest_temp_folder + object_name

                blob: Blob = self._bucket.blob(origin_folder)
                blob.download_to_filename(destination_file)

                return destination_file
            else:
                res: List[Tuple[str,str]] = []
                counter: int = 0 #used as a way to differentiate the files in name
                for source_folder in self._source_folders:
                    origin_folder: str = source_folder + object_name
                    destination_file: str = self._dest_temp_folder + \
                        str(counter) + "_" + object_name
                    counter += 1

                    blob: Blob = self._bucket.blob(origin_folder)
                    blob.download_to_filename(destination_file)

                    res.append((destination_file,
                        list(filter(lambda x: x != "", origin_folder.split("/")))[-2])
                    )
                return res
        except NotFound:
            raise FileNotFoundError("File with name " + object_name + " not found")

    def clear_cached_file(self, object_name: str) -> None:
        """
        Deletes downloaded file

        Args:
            object_name (str): file name

        Raises:
            FileNotFoundError: in case of OS error (propably caused by a file not found)
        """
        for file in os.listdir(self._dest_temp_folder):
            if object_name in file:
                error_code: int = subprocess.call(["rm", self._dest_temp_folder + file])
                if 0 != error_code:
                    raise FileNotFoundError("Cached file not found. Error code: " + str(error_code))