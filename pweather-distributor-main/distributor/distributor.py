import logging
from typing import Any, Dict, List, Optional, Tuple, Union
from typing_extensions import Final
import yaml, subprocess
from distributor.auxiliar.split_methods import SplitType, split_file
from downloader.downloader import downloader_interface
from strategy.strategy import TIME_INITIAL_DIM, TIME_VARIATION_DIM, dummy_metadata,\
low_res_grib_netcdf_metadata, metadata_strategy, grib_netcdf_metadata, round_robin_strategy,\
strategy_interface, time_interval_strategy
from uploader.uploader import uploader_interface

NETCDF_GRIB_STRAT: Final = "netcdf-grib"
LOW_RES_NETCDF_GRIB_STRAT: Final = "low-res-netcdf-grib"

"""
The code in this file is responsible for the whole distribution process.

The regular distribution mode and the split distribution mode are divided by
two separate functions.

The download_and_upload simply downloads the file and places it in a source.
The download_split_and_upload downloads, splits the file by multiple files and 
uploads each to a different node. This separation was done, as the split mode
was a feature implemented latter in the master thesis, and it became complicated
to implement both modes into a single method and simple and clear manner.

The download_and_upload is the only method that is capable of distributing files from
multiple sources at the same time. This was not implemented in the split mode for lack 
of time and because, in terms of distribution process itself, the split mode is not 
really practical in a full production environment outside of the academical context.
"""


class distributor:
    """
    Class responsible for downloading and organizing the
    individual object across the various nodes
    """
    def __init__(self, downloader: downloader_interface, 
                       uploader: uploader_interface, 
                       temp_dest_folder: str,
                       metadata_attrs: Dict[str, Any]) -> None:
        """
        Builds the distributor object

        Args:
            downloader (downloader_interface): Desired downloader
            uploader (uploader_interface): Desired uploader
            temp_dest_folder (str): Path of folder to be used to generate settings file
        """
        self._downloader: downloader_interface = downloader
        self._uploader: uploader_interface = uploader
        self._temp_dest_folder: str = temp_dest_folder
        """This is the existing distribution strategies. The ones starting
        by round-robin distribute the files in a round robin fashion and the
        one starting by timer-interval organize files by a time interval strategy.
        The last part of the strategy reffer to how the name of the files should be 
        processed
        """
        self._distribution_strategy: Dict[str, strategy_interface] = \
            {
                "round-robin-reduced": round_robin_strategy("reduced"),
                "round-robin-mock": round_robin_strategy("mock"),
                "round-robin-grib": round_robin_strategy("grib"),
                "time-interval-reduced": time_interval_strategy("reduced"),
                "time-interval-mock": time_interval_strategy("mock"),
                "time-interval-grib": time_interval_strategy("grib")
            }
        self._metadata_strategy: Dict[str, metadata_strategy] = \
            {
                NETCDF_GRIB_STRAT: grib_netcdf_metadata(metadata_attrs),
                LOW_RES_NETCDF_GRIB_STRAT: low_res_grib_netcdf_metadata(metadata_attrs),
                "dummy": dummy_metadata()
            }
        self._time_init_dim: str = metadata_attrs[TIME_INITIAL_DIM]
        self._time_var_dim: str = metadata_attrs[TIME_VARIATION_DIM]

        if self._temp_dest_folder[-1] != "/":
            self._temp_dest_folder += "/"

    def _create_settings_file(self, elements: List[str], metadata: Dict[str,Any], node_num: int) -> str:
        """
        Generates settings file to help index existing files

        Args:
            elements (List[str]): elements to be stored
            metadata (Dict[str,Any]): metadata to be added
            node_num (int): number of the used node

        Returns:
            str: path of the created file
        """
        file_name: str = self._temp_dest_folder + "settings_" + str(node_num) + ".yaml"
        with open(file_name, 'w') as file:
            settings_obj = {"settings": elements, "metadata": metadata}
            yaml.dump(settings_obj,file)
        return file_name
    
    def _clear_settings_file(self, file_path: str):
        """
        Deletes settings file

        Args:
            object_name (str): file name

        Raises:
            FileNotFoundError: in case of OS error (propably caused by a file not found)
        """
        error_code: int = subprocess.call(["rm", file_path])
        if 0 != error_code:
            raise FileNotFoundError("Settings file not found. Error code: " + str(error_code))


    def download_and_upload(self, strategy_name: str, metadata_strategy_name: str, 
        interval: Optional[Tuple[str,str]] = None, starting_point: str = None) -> List[List[str]]:
        """
        Distributes files across nodes with the given strategy

        Args:
            strategy_name (str): strategy name
            metadata_strategy_name (str): name of the strategy for the metadata generation
            interval (Optional[Tuple[str,str]]): interval of files to be downloaded or None, 
            if it should not be used
            starting_point (str): Optional argument. Set the starting point. 
                Used in a scenario where the distribution processed failed 
                and it is restarted
        """
        if not strategy_name in self._distribution_strategy:
            raise ValueError("Invalid argument: strategy " + strategy_name + " is not defined")
        
        should_download: bool = starting_point is None
        is_metadata_generated: bool = False
        metadata: List[Tuple[str,str,Dict[str,Any]]] = [] #tuple has the file path as the first attribute,
                                                          #the second as the "special information" (like 
                                                          # name of the data variable) and the third 
                                                          # attribute as the properties themselves

        #Section responsible for verifying what files have to be downloaded, and organize
        # how they should be distributed by the available nodes
        strategy: strategy_interface = self._distribution_strategy[strategy_name]
        metadata_strat: metadata_strategy = self._metadata_strategy[metadata_strategy_name] 
        elems: List[str] = self._downloader.get_object_list()
        elems.sort(key=self._sort_key)
        logging.debug(elems)
        elems = self._cut_list(elems, interval)
        logging.info(elems)
        number_of_nodes: int = len(self._uploader.get_nodes())

        elems_distributed: List[List[str]] = strategy.split(elems,number_of_nodes)

        """
        The distribution process is organized into the following steps:
            - Download the file
            - generate metadata file if necessary
            - upload the file
            - clear the files that have been placed in the temporary folder
            - after all it's done, upload the generated metadata file
        """
        for node_num in range(number_of_nodes):
            logging.info("Processing node " + str(node_num))
            for element in elems_distributed[node_num]:
                if not should_download and element == starting_point:
                    should_download = True

                if should_download:
                    logging.info("Processing file " + element)
                    """The downloaded file may have two different type:
                        - A string
                        - List of tuples of two strings
                        If it is a single string, it is the path of the downloaded file
                        If it is the List, then it has together the paths of the files 
                        together one some information used to distinguish that specific file.

                        The tuple is organized in the following manner:
                        (<path of the file>, <extra information>)
                        This second return type was created to allow the distribution tool
                        to distribute datasets from differents sources as if they were a single
                        dataset. This allows the distribution of different datasets each from 
                        different sources and data variables at the same time.

                        NOTE: this is not the best way to do it, since if anything is changed
                        in the structure, it may break the program. This was added as a last 
                        minute feature, and it should be changed in the future, where the logic
                        of the tuple in encapsulated to it's own object.
                    """
                    download_file_path: Union[str,List[Tuple[str,str]]] =\
                        self._downloader.get_object(element)
                    
                    if not is_metadata_generated and isinstance(download_file_path, str):
                        is_metadata_generated = True
                        metadata = [(download_file_path,None,metadata_strat.get_metadata(download_file_path))] 
                        #the second attribute is passed as None, as there is no "distinguishing name"

                    elif not is_metadata_generated and isinstance(download_file_path, list):
                        is_metadata_generated = True
                        for file_tuple in download_file_path:
                            metadata.append((file_tuple[0],file_tuple[1],metadata_strat.get_metadata(file_tuple[0])))
                    
                    self._uploader.upload_file(download_file_path,node_num, element)
                    self._downloader.clear_cached_file(element)
                else:
                    logging.info("Skipping file " + element)
                    
            if should_download:
                logging.info("Processing settings.yaml")
                for metadata_obj in metadata:
                    settings_path_file: str = self._create_settings_file(elems_distributed[node_num], metadata_obj[2], node_num)
                    self._uploader.upload_file([(settings_path_file,metadata_obj[1])], node_num, "settings.yaml")
                    self._clear_settings_file(settings_path_file)
            else:
                logging.info("Skipping settings.yaml")
                
        return elems_distributed


    def _sort_key(self, file_name: str) -> float:
        try:
            aux: List[str] = file_name.split("ERA5-")[1].split(".grib")[0].split(".nc")[0].split("-")
            return int(aux[1])*12 + int(aux[0])
        except Exception:
            raise ValueError("File presents an invalid format: " + file_name)

    def _cut_list(self, elems: List[str], interval: Optional[Tuple[str,str]]) -> List[str]:
        """Receives a full list of elements returned by the downloader, evaluates if it
        should be cut, in the case just a smaller interval was requested and returns 
        the list of elements as it should be

        Args:
            elems (List[str]): original list of elements received
            interval (Optional[Tuple[str,str]]): interval of wanted values, or None
            if no interval was requested

        Raises:
            ValueError: if the the files provided in the interval do not exist in 
            the list of elements
            ValueError: if the file provided for the end of interval actually comes
            sooner than the one in the beginning of the interval

        Returns:
            List[str]: the list of files properly processed
        """
        if interval is None:
            return elems
        else:
            if not (interval[0] in elems and interval[1] in elems):
                raise ValueError("One of the files given in interval does not exist in the dataset")
            else:
                begin: int = elems.index(interval[0])
                end: int = elems.index(interval[1])
                if end < begin:
                    raise ValueError("End file comes before from the start file")
                else:
                    return elems[begin:end+1]

    
    def download_split_and_upload(self, metadata_strategy_name: str, 
        split_type: SplitType, interval: Optional[Tuple[str,str]] = None, 
        starting_point: str = None) -> List[List[str]]:
        """
        Splits and distributes files across nodes with by a round robin strategy



        Args:
            strategy_name (str): strategy name
            metadata_strategy_name (str): name of the strategy for the metadata generation
            interval (Optional[Tuple[str,str]]): interval of files to be downloaded or None, 
            if it should not be used
            starting_point (str): Optional argument. Set the starting point. 
                Used in a scenario where the distribution processed failed 
                and it is restarted
        """
        should_download: bool = starting_point is None
        is_metadata_generated: bool = False
        elems: List[str] = self._downloader.get_object_list()
        elems.sort(key=self._sort_key)
        elems = self._cut_list(elems, interval)
        logging.info(elems)
        number_of_nodes: int = len(self._uploader.get_nodes())
        metadata_strat: metadata_strategy = self._metadata_strategy[metadata_strategy_name]
        current_node: int = 0
        metadata: Dict[str,Any] = {}

        for elem in elems:
            if not should_download and elem == starting_point:
                should_download = True

            if should_download:
                logging.info("Processing file " + elem)
                download_file_path: str = self._downloader.get_object(elem)
                if not is_metadata_generated:
                    is_metadata_generated = True
                    metadata = metadata_strat.get_metadata(download_file_path)

                """The distribution method here is going to differ from the originaly presented.
                The split_file function is going to first split, and then each file is placed in
                each worker node
                """
                for file in split_file(download_file_path, split_type, self._time_init_dim, \
                        self._time_var_dim,number_of_nodes):
                    if not self._uploader.does_node_have_file(file.file_name,current_node):
                        file_path: str = self._temp_dest_folder + "temp" + file.file_name
                        file.store_file(file_path)
                        
                        self._uploader.upload_file(file_path,current_node, file.file_name)
                        file.delete_stored_file()

                    current_node = (current_node + 1) % number_of_nodes
                
                self._downloader.clear_cached_file(elem)
            else:
                logging.info("Skipping file " + elem)
        
        logging.info("Processing settings.yaml files")
        for node_num in range(number_of_nodes):
            settings_path_file: str = self._create_settings_file(self._uploader.list_existing_files(node_num), metadata, node_num)
            self._uploader.upload_file(settings_path_file, node_num, "settings.yaml")
            self._clear_settings_file(settings_path_file)