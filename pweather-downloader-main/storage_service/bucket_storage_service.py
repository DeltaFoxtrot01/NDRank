from types import FunctionType
from google.auth.transport.requests import AuthorizedSession
from google.cloud import storage

class bucket_storage_service:

    def __init__(self, bucket_name: str):
        self._bucket_name = bucket_name
        self._client = storage.Client()
        self._bucket = self._client.get_bucket(self._bucket_name)
        self._url_template = ()

        self._transport = AuthorizedSession(
            credentials=self._client._credentials
        )
        

    def upload_file(self, object_name: str, filename_path: str, success_handler: FunctionType, failure_handler: FunctionType, timeout: int = 60*60*24):
        print("Transfering object ", object_name)
        try:
            blob = self._bucket.blob(object_name)
            blob.upload_from_filename(filename_path, timeout=timeout)
            success_handler()
        except ConnectionError:
            failure_handler()

    def download_file(self, object_name: str, destination_file: str, success_handler: FunctionType, failure_handler: FunctionType, timeout: int = 60*60*24):
        print("Downloading object ", object_name)
        try:
            blob = self._bucket.blob(object_name)
            blob.download_to_filename(destination_file)
            success_handler()
        except ConnectionError:
            failure_handler()