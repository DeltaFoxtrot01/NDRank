import logging
import subprocess
from typing import Optional

from uploader.uploader import uploader_interface


class file_uploader(uploader_interface):
    """Uploader that uploades the file to a given folder
    """
    def __init__(self, destination_folder_path: str) -> None:
        self._destination: str = destination_folder_path

        if self._destination[-1] != '/':
            self._destination += '/'

    def upload_object(self, object_name: str, file_path: str, sub_folder: Optional[str] = None) -> None:
        destination_file_name: str = self._destination
        if not sub_folder is None:
            destination_file_name += sub_folder
            if sub_folder[-1] != "/":
                destination_file_name += "/"

            if subprocess.run(["test","-d", destination_file_name]).returncode != 0:
                subprocess.run(["mkdir", destination_file_name], stdout=subprocess.DEVNULL)

        destination_file_name += object_name
        error_code: int = subprocess.call(["cp", file_path, destination_file_name], stdout=subprocess.DEVNULL)
        if error_code != 0:
            raise OSError("Unable to copy processed file into final destination. Error code: " + str(error_code))