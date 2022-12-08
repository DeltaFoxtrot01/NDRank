
class node:
    """
    Main class to represent a destination node for the uploader
    """
    pass


class mock_node(node):
    """
    Node used for mock uploader
    """
    def __init__(self, node_number: int) -> None:
        """
        Constructor for mock uploader, receives just a number

        Args:
            node_number (int): [description]
        """
        self._node_number: int = node_number

    @property
    def node_number(self) -> int:
        return self._node_number
    
class folder_node(node):
    """
    Constructor for folder uploader. Stores the name of the folder
    """
    def __init__(self, folder_name: str) -> None:
        """
        Constructor for the folder_node
        Args:
            folder_name (str): name of the folder
        """
        self._folder_name: str = folder_name
        if self._folder_name[-1] != "/":
            self._folder_name += "/"

    @property
    def folder_name(self) -> str:
        return self._folder_name