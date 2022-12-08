from typing import Final, Optional
from google.cloud.storage.bucket import Bucket
from uploader.uploader import uploader_interface
from google.cloud import storage

TIMEOUT: Final = 5*60


class gcp_bucket_uploader(uploader_interface):
    """
    Uploader for a google cloud bucket

    """
    def __init__(self, bucket_name: str, destination_folder: str) -> None:
        """
        Constructor for the google cloud uploader

        Args:
            bucket_name (str): name of the bucket
            destination_folder (str): name of the destination folder
        """
        self._client: storage.Client = storage.Client()
        self._bucket_name: str = bucket_name
        self._destination_folder: str = destination_folder
        if self._destination_folder[-1] != "/":
            self._destination_folder += "/"
        self._bucket: Bucket = self._client.get_bucket(self._bucket_name)

    def upload_object(self, object_name: str, file_path: str, sub_folder: Optional[str] = None) -> None:
        """
        Uploads object to google cloud bucket

        Args:
            object_name (str): final name of the object
            file_path (str): path of the file to store
        """
        destination_folder: str = self._destination_folder
        if not sub_folder is None:
            destination_folder += sub_folder
            if sub_folder[-1] != "/":
                destination_folder += "/" 
        blob = self._bucket.blob(destination_folder + object_name)
        blob.upload_from_filename(file_path, timeout=TIMEOUT)