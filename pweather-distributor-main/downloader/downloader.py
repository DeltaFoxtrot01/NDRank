from typing import List, Tuple,  Union


class downloader_interface:
    """
    Main interface to represent the service responsible for grabbing 
    the files from a source
    """

    def get_object_list(self) -> List[str]:
        """
        Fetches the list of all existing objects

        Returns:
            List[str]: [name of an object]
        """
        raise NotImplementedError("Method must be implemented")

    def get_object(self, object_name: str) -> Union[str,List[Tuple[str,str]]]:
        """
        Downloads an object given the name of it

        Args:
            object_name (str): name of the object to be fetched

        Returns:
            Union[str,List[Tuple[str,str]]]: path to the downloaded 
            object or multiple objects with the first attribute as 
            the path of the file and the second as a distinguishing 
            name (like the name of a data variable)  
        """
        raise NotImplementedError("Method must be implemented")

    def clear_cached_file(self, object_name: str) -> None:
        """
        Used for a situation where it is necessary to clear
        any cached downloaded file

        Args:
            object_name (str): name of the stored file

        Raises:
            FileNotFoundError: File does not exist
        """
        pass


class mock_downloader(downloader_interface):

    def __init__(self, folder_source: str, num_files: int) -> None:
        """
        Basic mock for file downloader, does not access any file

        Args:
            folder_source (str): file source (does not need to exist)
        """
        self._folder_source: str = folder_source
        self._files: List[str] = []
        for num in range(num_files):
            self._files.append("ERA5-1-" + str(num) + ".nc")

    def get_object_list(self) -> List[str]:
        return self._files

    def get_object(self, object_name: str) -> str:
        if not object_name in self._files:
            raise FileNotFoundError("File with name " + object_name + " not found")
        return self._folder_source + "/" + object_name
