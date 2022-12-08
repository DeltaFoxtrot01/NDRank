import subprocess
from typing import List, Optional, Tuple


class resolution_reducer_interface:
    """
    Basic resolution reducer interface. Already provides implementation to clear
    created files.
    """
    def __init__(self, temporary_folder: str):
        """
        Basic constructor. Sets up the list of temporary files to be
        cleaned later on.

        Args:
            temporary_folder (str): folder to create temporary files
        """

        self._temporary_folder: str = temporary_folder
        if self._temporary_folder[-1] != "/":
            self._temporary_folder += "/"
        
        self._temporary_files: List[str] = []

    def _add_temporary_file(self, file_path:str):
        """
        Adds file to list of files to be deleted later on

        Args:
            file_path (str): [description]
        """
        self._temporary_files.append(file_path)

    def reduce_resolution(self, source_file:str, file_name:str, previous_file: Optional[str] = None, 
        next_file: Optional[str] = None) -> Tuple[str,str]:
        """
        Execute the resolution reduction operation

        Args:
            source_file (str): source of the file
            file_name (str): name of the file
            previous_file (Optional[str], optional): Name of the previously downloaded file. Defaults to None.
            next_file (Optional[str], optional): Name of the next file to be processed. Defaults to None.

        Returns:
            Tuple[str,str]: path of the created file and name of the newly created file
        """
        pass

    def clear_cached_file(self):
        """
        Clears a file resulting from a reduced resolution

        Args:
            file_path (str): path of the created file
        """
        for file in self._temporary_files:
            error_code:int = subprocess.call(["rm", "-r", file])
            if error_code != 0:
                raise FileNotFoundError("Cached file not found. Error code: " + str(error_code))
        self._temporary_files = []

    def execute_at_end(self):
        """Method executed at the end of processing all files
        """



