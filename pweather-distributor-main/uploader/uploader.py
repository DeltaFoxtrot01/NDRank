from typing import List, Tuple, Union

from uploader.node import mock_node, node


class uploader_interface:
    """
    Main interface to represent the service responsible for uploading files to a node
    """
    
    def __init__(self):
        self._nodes: List[node] = []

    def add_nodes(self, nodes: List[node]) -> None:
        """
        Sets the list of destination nodes

        Args:
            nodes (List[node]): List of destination nodes
        """
        self._nodes = nodes

    def get_nodes(self) -> List[node]:
        """
        Returns the list of existing nodes

        Returns:
            List[node]: List of nodes
        """
        return self._nodes

    def upload_file(self, file_source: Union[str,List[Tuple[str,str]]], node_pos: int, 
        new_file_name: str = None) -> None:
        """
        Uploads a file to a specific source

        Args:
            file_source (Union[str,List[Tuple[str,str]]]): source of the file to submit (object resulting
            from the get_object method of the downloader_interface)
            node_pos (int): position of the destination node
            new_file_name (str): Optional argument. Name of the destination file (None to keep original name)
        """
        pass

    def does_node_have_file(self, file_name: str, node_pos: int) -> bool:
        """
        Verifies if the file is already inside the node

        Args:
            file_name (str): file name to be searched
            node_pos (int): node to verify 

        Returns:
            bool: true if the file exists in the node
        """

    def list_existing_files(self, node_pos: int) -> List[str]:
        """Returns a list of all existing files

        Returns:
            List[str]: name of all files
        """

class mock_uploader(uploader_interface):
    """
    Mock uploader for testing. Does not upload anything to anywhere
    """
    def __init__(self, number_of_dests: int):
        super().__init__()
        for number in range(number_of_dests):
            self._nodes.append(mock_node(number))

    def get_nodes(self) -> List[node]:
        return super().get_nodes()

    def upload_file(self, file_source: str, node_pos: int, new_file_name: str = None, extra_info: str = None) -> None:
        if not (0 <= node_pos < len(self._nodes)):
            raise ValueError("node_pos " + str(node_pos) +" does not exist")
        pass