import logging
from downloader.downloader import downloader_interface
import subprocess
from typing import Iterable, List
from typing_extensions import Final
from google.cloud.storage.blob import Blob #type: ignore

from google.cloud.storage.bucket import Bucket #type: ignore
from downloader.downloader import downloader_interface
from google.api_core.exceptions import NotFound
from google.cloud import storage #type: ignore

TIMEOUT: Final = 5*60

class gcp_bucket_downloader(downloader_interface):
    """
    Downloader for a google cloud bucket. 
    """

    def __init__(self, bucket_name: str, source_folder: str, temp_dest_folder:str) -> None:
        """
        Constructor for GCP bucket downloader. Sets up the connection 
        to the bucket

        It assumes files have the following name structures:
        ERA5-<month>-<year>.<extension>

        Args:
            bucket_name (str): name of the existing bucket
            source_folder (str): folder where files should be looked for
            temp_dest_folder (str): temporary destination folder for files
        """
        self._client: storage.Client = storage.Client()
        self._bucket_name: str = bucket_name
        self._source_folder: str = source_folder
        self._dest_temp_folder: str = temp_dest_folder
        
        if source_folder[-1] != "/":
            self._source_folder += "/"
        self._bucket: Bucket = self._client.get_bucket(self._bucket_name)

    def _key_to_sort_files(self,file_name:str) -> int:
        parts: List[str] = file_name.split(".")[0].split("-")[1:]
        month:int = int(parts[0])
        year: int = int(parts[1])

        return year * 100 + month


    def get_object_list(self) -> List[str]:
        """
        Lists all existing objects inside the bucket

        It also sorts the files by "time" order. It assumes that the
        files have the following format:
            "ERA5-<MONTH>-<YEAR>.nc|grib"

        Returns:
            List[str]: list of the name of all existing objects
        """
        res: List[str] = []
        iterator: Iterable = self._client.list_blobs(bucket_or_name=self._bucket_name,
                                                     prefix=self._source_folder,
                                                     timeout=TIMEOUT)
        for object in iterator:
            file_name: str = object.name
            file_name = file_name.replace(self._source_folder,"",1)
            if file_name != "":
                res.append(file_name)
        
        res.sort(key=lambda x: self._key_to_sort_files(x))

        return res

    def get_object(self, object_name: str) -> str:
        """
        Download objects from 

        Args:
            object_name (str): [description]

        Raises:
            FileNotFoundError: If a "NotFound" error is returned

        Returns:
            str: [description]
        """
        try:
            origin_folder: str = self._source_folder + object_name
            destination_file: str = self._dest_temp_folder + object_name

            blob: Blob = self._bucket.blob(origin_folder)
            blob.download_to_filename(destination_file, timeout=TIMEOUT)

            return destination_file
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
        error_code: int = subprocess.call(["rm", object_name])
        if 0 != error_code:
            logging.error("Cached file not found. Error code: " + str(error_code))