from typing import List

class downloader_interface:
    """
    Main interface to represent the service responsible for grabbing 
    the files from a source
    """

    def get_object_list(self) -> List[str]:
        """
        Fetches the list of all existing objects

        DOES NOT SORT BY DEFAULT
        Returns:
            List[str]: list of files available to be downloaded
        """
        raise NotImplementedError("Method must be implemented")

    def get_object(self, object_name: str) -> str:
        """
        Downloads an object given the name of it

        Args:
            object_name (D): name of the object to be fetched

        Returns:
            str: path to the downloaded object
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
