
from protocol.protocol_pb2 import DatasetProperties


class InputFileProperties:
    """Dto used to represent the required metadata for each input file
    """

    def __init__(self, file_name: str, file_size: int, data_variable: str) -> None:
        """
        Basic constructor for dto

        Args:
            file_name (str): Name of the given file
            file_size (int): Size of the file
            data_variable (str): Data variable used in the file
        """
        self._file_name: str = file_name
        self._file_size: int = file_size
        self._data_var: str = data_variable

    @property
    def file_name(self) -> str:
        return self._file_name

    @property
    def file_size(self) -> int:
        return self._file_size

    @property
    def data_variable(self) -> str:
        return self._data_var

    def get_file_without_path(self) -> str:
        """Returns only the name of the file, not the given path

        Returns:
            str: name of the file
        """
        return self._file_name.split("/")[-1]

    def __repr__(self) -> str:
        return self._file_name + "\tsize: " + str(self._file_size) + \
            "\tdata variable: " + str(self._data_var)


def factory_InputFileProperties(object: DatasetProperties, data_variable: str) -> InputFileProperties:
    """Simple factory function to build an InputFileProperties object
    from a DatasetProperties object

    Args:
        object (DatasetProperties): original object
        data_variable (str): data variable that the file contains

    Returns:
        InputFileProperties: tranformed object
    """
    return InputFileProperties(object.file_name, object.size_of_dataset, data_variable)