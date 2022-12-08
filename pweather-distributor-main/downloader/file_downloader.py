from typing import List
from downloader.downloader import downloader_interface
import subprocess

class file_downloader(downloader_interface):
    """
    Local file downloader (used for development and mounted disks).
    It takes a folder as a source and "downloads" the files from that
    given folder.
    """

    def __init__(self, folder_source: str) -> None:
        """
        Constructor for file downloader. Used as a local
        downloader for development

        Args:
            folder_source (str): file source of the objects
        """
        self._source: str = folder_source

    def get_object_list(self) -> List[str]:
        """
        Lists all objects that exist in the folder

        Returns:
            List[str]: List of all objects existing in the folder
        """
        elems: List[str] = subprocess.check_output(['ls', self._source], shell=False)\
                                     .decode('utf-8')\
                                     .split("\n")
        elems.pop(-1)   #last element is an empty string
        return elems

    def get_object(self, object_name: str) -> str:
        """Makes a copy of the file that exists in the folder

        Args:
            object_name (str): name of the file

        Returns:
            str: path of the copied file
        """
        elems: List[str] = self.get_object_list()
        if not object_name in elems:
            raise FileNotFoundError("Object with name " + object_name + " was not found")
        return self._source + '/' + object_name
        


    