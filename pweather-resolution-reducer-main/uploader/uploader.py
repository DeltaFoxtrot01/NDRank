import logging
from typing import List, Optional, Tuple

class uploader_interface:
    """
    Main interface to represent a service to upload a file
    """

    def upload_object(self, object_name: str, file_path: str, sub_folder: Optional[str] = None) -> None:
        """
        Uploads an object given the name of it

        Args:
            object_name (str): final name of the object to be uploaded
            file_path (str): path of the file to be uploaded
            sub_folder (Optional[str]): sub folder to store the file
        """
        raise NotImplementedError("Method must be implemented")

    def upload_objects(self, object_name_file_path_pairs: List[Tuple[str,str,str]]) -> None:
        """Uploads multiple files. Used for the "split by data variable" process

        Args:
            object_name_file_path_pairs (List[Tuple[str,str,str]]): list of files to be uploaded.
            The list of files is organized in tuples with the following parameters:
            (path of the created file, intended name of the file, data variable containing it)
        """
        for file in object_name_file_path_pairs:
            self.upload_object(file[1],file[0],file[2])

class none_uploader(uploader_interface):
    """It uploads nothing to nowhere
    """

    def upload_object(self, object_name: str, file_path: str, sub_folder: Optional[str] = None) -> None:
        pass