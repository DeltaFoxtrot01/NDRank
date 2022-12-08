from datetime import datetime, timedelta
from typing import Dict, Final, Iterable, List, Tuple

from google.cloud.storage.blob import Blob #type: ignore
from google.cloud.storage.bucket import Bucket #type: ignore
from google.cloud import storage #type: ignore
from google.api_core.exceptions import NotFound #type: ignore
from properties_processor.processor import SettingsResults

TIMEOUT: Final = 5*60

class gcp_bucket_downloader:
    """
    Downloader for a google cloud bucket
    """

    def __init__(self, properties: SettingsResults) -> None:
        """
        Constructor for GCP bucket downloader. Sets up the connection 
        to the bucket

        It assumes files have the following name structures:
        ERA5-<month>-<year>.<extension>

        Args:
            properties (SettingsResults): processed properties from properties file
        """
        self._client: storage.Client = storage.Client()
        self._bucket_name: str = properties.bucket_name
        self._source_folder: str = properties.origin_folder
        self._dest_folder: str = properties.destination_folder
        
        if self._source_folder[-1] != "/":
            self._source_folder += "/"

        if self._dest_folder[-1] != "/":
            self._dest_folder += "/"

        self._bucket: Bucket = self._client.get_bucket(self._bucket_name)

    def _key_to_sort_files(self,file_name:str) -> int:
        parts: List[str] = file_name.split(".")[0].split("-")[1:]
        month:int = int(parts[0])
        year: int = int(parts[1])

        return year * 100 + month


    def get_object_list(self) -> List[str]:
        """
        Lists all existing objects inside the bucket

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
            str: path of the downloaded file
        """
        try:
            origin_folder: str = self._source_folder + object_name
            destination_file: str = self._dest_folder + object_name

            blob: Blob = self._bucket.blob(origin_folder)
            blob.download_to_filename(destination_file, timeout=TIMEOUT)

            return destination_file
        except NotFound:
            raise FileNotFoundError("File with name " + object_name + " not found")


def download_files(props: SettingsResults) -> Dict[Tuple[int,int], str]:
    def _get_key(date: datetime) -> str:
        return "ERA5-" + str(date.month) + "-" + str(date.year) + ".nc"

    def _get_month_year_pair(date: datetime) -> Tuple[int,int]:
        return (date.month, date.year)

    downloader: gcp_bucket_downloader = gcp_bucket_downloader(props)
    files: List[str] = downloader.get_object_list()

    res: Dict[Tuple[int,int], str] = {}

    for date in props.wanted_dates:
        file_name: str = _get_key(date)
        month_year_pair: Tuple[int,int] = _get_month_year_pair(date)

        if not file_name in files:
            raise ValueError("Date does not exist in dataset: " + str(date))
        
        if not month_year_pair in res:
            res[month_year_pair] = downloader.get_object(file_name)
        
        if date.day == 1:
            date = date - timedelta(days=1)
            file_name = _get_key(date)
            month_year_pair = _get_month_year_pair(date)
            
            if not file_name in files:
                raise ValueError("Date does not exist in dataset: " + str(date))
            
            if not month_year_pair in res:
                res[month_year_pair] = downloader.get_object(file_name)
    
    return res
        
