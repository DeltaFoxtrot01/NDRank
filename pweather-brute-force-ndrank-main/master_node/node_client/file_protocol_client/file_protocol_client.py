import logging
import os
import socket
from typing import BinaryIO, Final, Optional

from protocol.protocol_pb2 import FilePortMapping


BUFFER_SIZE: Final = 4096

class FileTransferInstanceClient:
    """Client for file transfer.
    Responsible for transfering a single file
    """

    def __init__(self, port_mapping: FilePortMapping, host_address: str) -> None:
        """
        Main constructor that sets up the socket communication

        Args:
            port_mapping (FilePortMapping): mapping of port to reffered file
            host_address (str): IP or DNS name of worker node
        """
        self._port: int = port_mapping.port
        self._file_name: str = port_mapping.file
        self._host_address: str = host_address
        self._exception: Optional[Exception] = None

        logging.debug(str(self._port) + "\t" + self._file_name + "\t" + self._host_address)

        ip: str = socket.gethostbyname(host_address)
        self._socket: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((ip, self._port))

    def upload(self) -> None:
        """Executes the upload process via socket
        """
        try:
            fp: BinaryIO = open(self._file_name, mode="rb")

            self._socket.sendfile(fp)

            self._socket.shutdown(socket.SHUT_RDWR)
            self._socket.close()
            fp.close()
        except Exception as e:
            self._exception = e

    
    @property
    def exception(self) -> Optional[Exception]:
        return self._exception