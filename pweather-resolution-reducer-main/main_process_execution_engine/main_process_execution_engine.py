import glob
import logging
import subprocess
from datetime import datetime
from typing import Final, List, Optional, Tuple
from xmlrpc.client import Boolean
from multiprocessing.sharedctypes import Value
from split_methods.split import clean_aux_split_files, split_file_by_data_variables
from uploader.uploader import uploader_interface
from downloader.downloader import downloader_interface
from resolution_reducer_tools.resolution_reducer import resolution_reducer_interface

LOG_FOLDER_PATH: Final = "./log_files/"

FILE_ALL: Final = "all"
FILE_INTERVAL: Final = "interval"
FILE_SPECIFIC: Final = "specific"
MONTH_SPECIFIC: Final = "months"

#operation types
SPLIT_BY_VARS: Final = "split-by-vars"
FILTER_FILES: Final = "filter-files"

class main_process_execution_engine:
    """
    Main class responsible for downloading all existing objects from the given source,
    execute the process of converting and reducing the received grid of the file and 
    upload the newly created file to a given source
    """

    def __init__(self, downloader: downloader_interface, 
                       uploader: uploader_interface,
                       temporary_folder_path: str) -> None:
        """
        Basic constructor for the main execution engine

        Args:
            downloader (downloader_interface): downloader to be used
            uploader (uploader_interface): uploader to be used
            temporary_folder_path (temporary_folder_path): path for temporary folder
        """
        self._downloader: downloader_interface = downloader
        self._uploader: uploader_interface = uploader
        self._temporary_folder_path: str = temporary_folder_path
        self._resolution_reducer_pipeline: List[resolution_reducer_interface] = []

        if self._temporary_folder_path[-1] != '/':
            self._temporary_folder_path+='/'
    
    def _clean_idx_files(self) -> None:
        files: List[str] = [i for i in glob.glob(self._temporary_folder_path + "*")]
        if len(files) != 0:
            error_code:int = subprocess.call(["rm", "-r"] + files)
            if error_code != 0:
                raise FileNotFoundError("Cached file not found. Error code: " + str(error_code))

    def add_reducer(self, resolution_reducer: resolution_reducer_interface) -> None:
        """
        Adds a resolution reducer to the pipeline

        Args:
            resolution_reducer (resolution_reducer_interface): resolution reducer to be added
        """
        self._resolution_reducer_pipeline.append(resolution_reducer)

    def _filter_list(self, 
                     mode: str, 
                     original_file_names: List[str],
                     starting_file: str = None, 
                     ending_file: str = None,
                     specific_files: List[str] = None,
                     months: List[int] = None) -> List[int]:
        """
        Receives a full list of elements and filters the list
        according the mode provided mode (either all, interval, 
        specific or months)

        Args:
            mode (str): name of the mode
            original_file_names (List[str]): List of files returned by the downloader
            starting_file (str, optional): Starting file for the interval. Defaults to None.
            ending_file (str, optional): Ending file for the interval. Defaults to None.
            specific_files (List[str], optional): Specific files that should be downloaded. Defaults to None.
            months (List[int], optional): Months that should be downloaded. Defaults to None.

        Returns:
            List[int]: List of indexes of the original file filtered according to the given mode
        """
        file_name_indexes: List[int] = []
        #deal with the files that should be downloaded
        if mode == FILE_ALL:
            file_name_indexes = list(range(len(original_file_names)))
        elif mode == FILE_INTERVAL:
            if starting_file is None or ending_file is None:
                raise ValueError("In Internal mode, starting_file and ending_file must be defined")

            started: Boolean = False
            ended: Boolean = False

            for index in range(len(original_file_names)):
                if original_file_names[index] == starting_file:
                    started = True

                if started:
                    file_name_indexes.append(index)

                if original_file_names[index] == ending_file:
                    ended = True
                    break
            
            if not started:
                raise ValueError("Starting file does not exist")
            if not ended:
                raise ValueError("Ending File does not exist") 

        elif mode == FILE_SPECIFIC:
            if specific_files is None:
                raise ValueError("specific_files must be defined in specific mode")

            for file in specific_files:
                try:
                    index = original_file_names.index(file)
                    file_name_indexes.append(index)
                except ValueError:
                    raise ValueError("File " + file + " was not found")    
        
        elif mode == MONTH_SPECIFIC:
            if months is None:
                raise ValueError("months must be defined in months mode")
            
            for file in original_file_names:
                try:
                    month: int = int(file.split("-")[1])
                    if month in months:
                        index = original_file_names.index(file)
                        file_name_indexes.append(index)
                except ValueError:
                    raise ValueError("File " + file + " was not found")
        else:
            raise ValueError("Invalid file download mode")

        return file_name_indexes

    def execute(self, mode: str, 
                      operation_type: str,
                      starting_file: str = None, 
                      ending_file: str = None,
                      download_past_and_future: bool = False,
                      specific_files: List[str] = None,
                      months: List[int] = None) -> None:
        """
        Function to execute the full creation of the lower resolution dataset.
        It may iterate the full dataset or only from a specific file name.

        Args:
            mode (str): which and how should the files be downloaded. 
            If it should be all the files, a certain interval, a specific choice, etc.
            operation_type (str): if the files should be filtered by a selection of filters
            or be split by data variables
            starting_file (str,): Starting point file. Defaults to None.
            ending_file (str): Ending file. Defaults to None.
            download_past_and_future (bool): If the previous and next file should also
            be downloaded as past and future. Defaults to False.
            specific_files (List[str]): List of specific files to be downloaded. Defaults to None.
            months (List[int]): List of specific months to be downloaded. Defaults to None.
        """

        logging.basicConfig(level=logging.DEBUG)
        original_file_names: List[str] = self._downloader.get_object_list()
        file_name_indexes: List[int] = []

        #deal with the files that should be downloaded
        file_name_indexes = \
            self._filter_list(mode,original_file_names,starting_file,ending_file,specific_files,months)

        logging.debug(original_file_names)
        logging.debug(file_name_indexes)
        logging.info("Files to be downloaded: " + str(list(map(lambda i: original_file_names[i], file_name_indexes))))

        previous_index: Optional[int] = None

        past_downloaded_file: Optional[str] = None
        future_downloaded_file: Optional[str] = None
        downloaded_file: Optional[str] = None

        #Section for the execution of the pipeline
        #steps goes as follows:
        #1) download the file
        #2) execute each step of the pipeline, cleaning the files from the previous steps
        #3) upload the file
        #4) clean the remaining files
        for index in file_name_indexes:

            reduced_file: str
            new_file_name: str
            new_files: List[Tuple[str,str,str]]
            previous_reducer: Optional[resolution_reducer_interface] = None

            #step 1: download the file
            if download_past_and_future:
                """In a situation where it is necessary to download the past and future files,
                an optimization that can be made is to set the future as the present and the present
                as the past. This avoids unnecessary downloads.

                This may not be possible in situations where specific files are being downloaded, 
                for example
                """
                if not previous_index is None:
                    logging.debug("going to download past and future " + str(previous_index) + "," + str(index))
                    if previous_index == index - 1:
                        if not past_downloaded_file is None:
                            self._downloader.clear_cached_file(past_downloaded_file)
                        past_downloaded_file = downloaded_file
                        downloaded_file = future_downloaded_file
                        if index < len(original_file_names) - 1:
                            try:
                                future_downloaded_file = self._downloader.get_object(original_file_names[index+1])
                            except ConnectionError as e:
                                logging.error("DOWNLOAD: Error downloading file: " + original_file_names[index+1] + "\t" + str(e))
                        previous_index = index
                    else:
                        if not past_downloaded_file is None:
                            self._downloader.clear_cached_file(past_downloaded_file)
                        self._downloader.clear_cached_file(downloaded_file)
                        if not future_downloaded_file is None:
                            self._downloader.clear_cached_file(future_downloaded_file)
                        previous_index = None

                if previous_index is None:
                    try:
                        downloaded_file = self._downloader.get_object(original_file_names[index])
                    except ConnectionError as e:
                        logging.error("DOWNLOAD: Error downloading file: " + original_file_names[index] + "\t" + str(e))       
                    if index > 0:
                        try:
                            past_downloaded_file = self._downloader.get_object(original_file_names[index-1])
                        except ConnectionError as e:
                            logging.error("DOWNLOAD: Error downloading file: " + original_file_names[index-1] + "\t" + str(e))
                    if index < len(original_file_names) - 1:
                        try:
                            future_downloaded_file = self._downloader.get_object(original_file_names[index+1])
                        except ConnectionError as e:
                            logging.error("DOWNLOAD: Error downloading file: " + original_file_names[index+1] + "\t" + str(e))

                    previous_index = index
            else:
                try:
                    if not downloaded_file is None:
                        self._downloader.clear_cached_file(downloaded_file)
                    downloaded_file = self._downloader.get_object(original_file_names[index])
                except ConnectionError as e:
                    logging.error("DOWNLOAD: Error downloading file: " + original_file_names[index] + "\t" + str(e))
            logging.debug("Previous index after change: " + str(previous_index))
            logging.info("Handling file " + original_file_names[index])
            #step 2: execute each step of the pipeline, cleaning the files from the previous steps
            try:
                if operation_type == FILTER_FILES:
                    reduced_file = downloaded_file
                    new_file_name = original_file_names[index]
                    for reducer in self._resolution_reducer_pipeline:
                        reduced_file, new_file_name = reducer.reduce_resolution(reduced_file, new_file_name, past_downloaded_file, future_downloaded_file)
                        if not previous_reducer is None:
                            previous_reducer.clear_cached_file()
                        previous_reducer = reducer
                elif operation_type == SPLIT_BY_VARS:
                    new_file_name = original_file_names[index]
                    new_files = split_file_by_data_variables(downloaded_file, new_file_name, self._temporary_folder_path)
                else:
                    raise ValueError("Invalid operation type")

            except Exception as e:
                logging.error("FILE: Error treating file: " + original_file_names[index] + "\t" + str(e))
                logging.exception(e)
                logging.info("Skipping file " + original_file_names[index])
                continue

            #3) upload the file
            try:
                if operation_type == FILTER_FILES:
                    self._uploader.upload_object(new_file_name, reduced_file)
                    if not previous_reducer is None:
                        previous_reducer.clear_cached_file()
                elif operation_type == SPLIT_BY_VARS:
                    self._uploader.upload_objects(new_files)
                    clean_aux_split_files(self._temporary_folder_path)
                else: 
                    raise ValueError("Invalid operation type")
            except Exception as e:
                logging.error("UPLOAD: Error uploading file: " + reduced_file + "\t" + str(e))
                logging.exception(e)
            

        if not past_downloaded_file is None:
            self._downloader.clear_cached_file(past_downloaded_file)
        if not downloaded_file is None:
            self._downloader.clear_cached_file(downloaded_file)
        if not future_downloaded_file is None:
           self._downloader.clear_cached_file(future_downloaded_file)
        
        for reducer in self._resolution_reducer_pipeline:
            reducer.execute_at_end()