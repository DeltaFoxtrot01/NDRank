import logging
from multiprocessing import Lock
import os
import socket
import subprocess
import tempfile
import threading
import exceptiongroup
from typing import BinaryIO, Final, List, Tuple
from BitVector import BitVector # type: ignore
from controller.file_protocol.dtos.dataset_properties import InputFileProperties 

from controller.file_protocol.exceptions import OutOfSocketPortsError
from protocol.protocol_pb2 import FilePortMapping

EMPTY_RETRY_TIMES: Final = 5 #number of times straight it can be tolerated for a packet to come empty
DEFAULT_TIMEOUT: Final = 10000 #timeout for socket messages
BUFFER_SIZE: Final = 4096

"""
Manages the server logic to transfer files over a TCP socket communication.

To manage the socket communications it is necessary to instantiate an 
FileProtocol object and provide the required parameters.

This will manage the existing ports for the created sockets and open the existing
sockets.

It will return an instance of an object from the type FileTransferInstance.

The FileTransferInstance manages the opened socket communications and the required
threads for the process.

The FileTransferInstance creates a thread per socket and each socket is responsible
for the transfer of one single file.

After the process has finished, it is necessary to pass the FileTransferInstance to
the FileProtocol in order to release the allocated ports.
"""

class FileTransferInstance:
    """Object responsible for managing all threads dealing with the transfer process
    
    Object responsible for a currently executing transfer of files after the ports have 
    been allocated. 
    """

    def __init__(self, host: str, chunk_size: int, temp_file_path: str, port_mapping: List[Tuple[InputFileProperties, int]]):
        """
        
        Args:
            host (str): [description]
            chunk_size (int): size of the chunk that will be read from disk
            temp_file_path (str): [description]
            port_mapping (List[Tuple[InputFileProperties, int]]): [description]
        """
        self._host: str = host
        self._chunk_size: int = chunk_size
        self._temporary_file_path: str = temp_file_path
        self._port_mapping: List[Tuple[InputFileProperties, int]] = port_mapping
        self._threads: List[threading.Thread] = []
        self._resulting_files: List[Tuple[str, InputFileProperties]] = []

        self._resulting_files_lock: threading.Lock = threading.Lock()
        self._exception_lock: threading.Lock = threading.Lock()
        self._exception_list: List[Exception] = []

    @property
    def port_mapping(self) -> List[Tuple[InputFileProperties,int]]:
        return self._port_mapping
    
    def _init_socket(self, file_properties: InputFileProperties, file_port: int) -> socket.socket:
        """
        Creates the socket for the file transfer

        Args:
            file_properties (InputFileProperties): file properties
            file_port (int): port to be used

        Returns:
            socket.socket: created socket
        """
        logging.debug(repr(file_properties) + "\n" + str(file_port))

        # block to create the socket
        ip: str = socket.gethostbyname(self._host)
        file_socket: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        file_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        file_socket.settimeout(DEFAULT_TIMEOUT)
        file_socket.bind((ip,file_port))
        file_socket.listen()
        
        return file_socket 

    def _init_file_transfer(self, file_properties: InputFileProperties, file_socket: socket.socket) -> None:
        """Manages the socket transfer process
        It's divided in 3 steps:
        - create an empty file
        - download the file
        - close the socket

        Args:
            file_properties (InputFileProperties): _description_
            file_port (int): _description_
        """
        total_size: int = file_properties.file_size
        downloaded: int = 0

        conn: socket.socket
        conn, _ = file_socket.accept()

        # create an empty file to write the content
        file_name: str = file_properties.get_file_without_path()

        file_descriptor, output_file = tempfile.mkstemp(suffix='_' + file_name, prefix="temp_", dir=self._temporary_file_path)
        os.close(file_descriptor)
        fp: BinaryIO = open(output_file, "wb")

        # counts the number of times straight a packet came empty
        times_packet_came_empty: int = 0

        # download the file
        while downloaded < total_size and total_size != 0:
            received_payload: bytes 
            try:
                received_payload = conn.recv(self._chunk_size)
            except InterruptedError as e:
                conn.shutdown(socket.SHUT_RDWR)
                file_socket.shutdown(socket.SHUT_RDWR)
                conn.close()
                file_socket.close()
                fp.close()
                self._exception_lock.acquire()
                self._exception_list.append(e)
                self._exception_lock.release()
                return

            if len(received_payload) == 0:
                times_packet_came_empty += 1
                if times_packet_came_empty > EMPTY_RETRY_TIMES:
                    # close all sockets
                    conn.shutdown(socket.SHUT_RDWR)
                    file_socket.shutdown(socket.SHUT_RDWR)
                    conn.close()
                    file_socket.close()
                    fp.close()

                    self._exception_lock.acquire()
                    self._exception_list.append(TimeoutError("Received to many packets empty at a time"))
                    self._exception_lock.release()
            else:
                times_packet_came_empty = 0
                fp.write(received_payload)
            
            downloaded += len(received_payload)


        # close all sockets
        conn.shutdown(socket.SHUT_RDWR)
        file_socket.shutdown(socket.SHUT_RDWR)
        conn.close()
        file_socket.close()
        fp.close()

        self._resulting_files_lock.acquire()
        self._resulting_files.append((output_file,file_properties))
        self._resulting_files_lock.release()

    def open_sockets(self) -> None:
        """Creates the required threads for each socket to start the transfer
        process
        """
        for mapping in self._port_mapping:
            file_socket: socket.socket = self._init_socket(mapping[0],mapping[1])
            thread_obj: threading.Thread = \
                threading.Thread(target=self._init_file_transfer, args=(mapping[0],file_socket))
            self._threads.append(thread_obj)
            thread_obj.setDaemon(True)
            thread_obj.start()

    def wait_for_termination(self) -> List[Tuple[str,InputFileProperties]]:
        """Waits for all threads to finish the transfer process

        Returns:
            List[str]: Created files
        """
        for thread in self._threads:
            thread.join()

        return self._resulting_files
    
    def delete_files(self):
        """Deletes all created files
        """
        for file in self._resulting_files:
            subprocess.call(["rm", file[0]])

    def verify_if_exceptions_were_thrown(self) -> None:

        """Verifies if the executed threads threw any exception

        Raises:
            exceptiongroup.ExceptionGroup: if the threads threw any
            exception
        """
        if len(self._exception_list) != 0:
            raise exceptiongroup.ExceptionGroup(
                "Exceptions thrown from sockets dealing with file transfer", 
                self._exception_list)

def factory_FilePortMapping(mapping: Tuple[InputFileProperties, int]) -> FilePortMapping:
    """Simple factory method to build a FilePortMapping object

    Args:
        mapping (Tuple[InputFileProperties, int]): mapping produced by FileProtocol

    Returns:
        FilePortMapping: resulting object
    """
    return FilePortMapping(file=mapping[0].file_name, port=mapping[1])

class FileProtocol:
    """Class responsible for managing all opened TCP socket communications
    """

    def __init__(self, host: str, from_port: int, to_port: int, temp_folder_path: str) -> None:
        """Basic constructor to store all required metadata to open and 
        manage the required connections

        Args:
            host (str): host to bind required sockets
            from_port (int): starting port that can be used
            to_port (int): ending port that can be used

        Raises:
            ValueError: if 'to_port' is smaller or equal to 'from_port'
        """
        if to_port <= from_port:
            raise ValueError("'to_port' must be bigger than 'from_port'")
        self._host: str = host
        self._from_port: int = from_port
        self._to_port: int = to_port
        self._chunk_size: int = BUFFER_SIZE
        self._temporary_folder_path: str = temp_folder_path
        self._ports_allocated: BitVector = BitVector(size=to_port - from_port)
        self._port_lock: threading.Lock = threading.Lock()

        if self._temporary_folder_path[-1] != '/':
            self._temporary_folder_path += '/'

    def _allocate_ports(self, files: List[InputFileProperties]) -> List[Tuple[InputFileProperties, int]]:
        """Allocates port to be later used in each file

        Args:
            files (List[InputFileProperties]): File to have ports allocated

        Raises:
            OutOfSocketPortsError: Thrown if it is unable to map any port

        Returns:
            List[Tuple[InputFileProperties, int]]: mapping between the given files and the 
            allocated port
        """
        res: List[Tuple[InputFileProperties, int]] = []
        current_port: int = self._from_port
        file_index: int = 0

        self._port_lock.acquire()

        while current_port < self._to_port:
            if not self._ports_allocated[current_port - self._from_port]:
                self._ports_allocated[current_port-self._from_port] = True
                res.append((files[file_index], current_port))
                file_index += 1

                if not file_index < len(files):
                    self._port_lock.release()
                    return res
            current_port += 1

        self._port_lock.release()
        raise OutOfSocketPortsError("Unable to map all files to a port")

    def release_ports(self, file_tranfer_obj: FileTransferInstance) -> None:
        """Deallocates ports given the mapping produced by 
        _allocate_ports

        Args:
            file_ports (List[Tuple[InputFileProperties,int]]): result provided by 
            _allocate_ports method
        """
        file_ports: List[Tuple[InputFileProperties,int]] = file_tranfer_obj.port_mapping
        self._port_lock.acquire()
        for mapping in file_ports:
            self._ports_allocated[mapping[1] - self._from_port] = False
        self._port_lock.release()
    
    def open_sockets(self, files: List[InputFileProperties]) -> FileTransferInstance:
        """Opens a socket connection per file, returning port mapping

        Args:
            files (List[str]): files to be transfered latter on

        Returns:
            List[Tuple[str, int]]: port mapping per file
        """
        file_mapping: List[Tuple[InputFileProperties,int]] = self._allocate_ports(files)
        file_transfer_obj: FileTransferInstance = \
            FileTransferInstance(self._host, self._chunk_size, self._temporary_folder_path, file_mapping)
        file_transfer_obj.open_sockets()

        return file_transfer_obj

    def wait_for_files_to_transfer(self, file_tranfer_obj: FileTransferInstance) -> List[Tuple[str,InputFileProperties]]:
        """Waits for all threads to finish the transfer process and releases
        all ports that have been alocated

        Args:
            file_tranfer_obj (FileTransferInstance): object returned by the "open_sockets" method

        Returns:
            List[str]: path of the files downloaded
        """
        res: List[Tuple[str,InputFileProperties]]
        res = file_tranfer_obj.wait_for_termination()
        self.release_ports(file_tranfer_obj)
        file_tranfer_obj.verify_if_exceptions_were_thrown()

        return res

