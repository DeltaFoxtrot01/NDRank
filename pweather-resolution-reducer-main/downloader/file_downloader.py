from typing import List
from downloader.downloader import downloader_interface
import subprocess

class file_downloader(downloader_interface):
    """
    Local file downloader (used for development to work with folders alone)
    """

    def __init__(self, folder_source: str) -> None:
        """
        Constructor for file downloader. Used as a local
        downloader for development

        Args:
            folder_source (str): file source of the objects
        """
        self._source: str = folder_source
        if self._source[-1] != '/':
            self._source += '/'

    def get_object_list(self) -> List[str]:
        """
        Lists all objects that exist in the folder

        Returns:
            List[str]: List of all objects existing in the folder
        """
        elems: List[str] = subprocess.check_output(['ls', self._source], shell=False)\
                                     .decode('utf-8')\
                                     .split("\n")
        #Filters index files and the empty file
        elems = list(filter(lambda name : (".grib" in name or ".nc" in name) and not (".idx" in name), elems))  
        return elems

    def get_object(self, object_name: str) -> str:
        """
        Used for a situation where it is necessary to clear
        any cached downloaded file

        Args:
            object_name (str): name of the stored file

        Raises:
            FileNotFoundError: File does not exist
        """
        elems: List[str] = self.get_object_list()
        if not object_name in elems:
            raise FileNotFoundError("Object with name " + object_name + " was not found")
        return self._source + object_name

    