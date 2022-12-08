import subprocess
from typing import List, Tuple, Union
from aux_functions.aux_functions import key_function_for_file
from uploader.node import folder_node
from uploader.uploader import uploader_interface


class local_file_uploader(uploader_interface):
    """Implementation of the local file uploader

    """
    
    def add_nodes(self, nodes: List[folder_node]) -> None:
        """
        Adds folder nodes

        Args:
            nodes (List[folder_node]): folder nodes
        """
        super().add_nodes(nodes)

    @property
    def folder_nodes(self) -> List[folder_node]:
        return self._nodes

    def upload_file(self, file_source: Union[str,List[Tuple[str,str]]], node_pos: int, 
        new_file_name: str = None) -> None:
        """
        Uploads a file to a specific source

        Args:
            file_source (Union[str,List[Tuple[str,str]]]): source of the file to submit where it is either a
            path for the object to be submitted, or a list of tuples with strings where the first element is 
            the path of the file and the second is used to create a sub folder (object resulting from the 
            get_object method of the downloader_interface)
            node_pos (int): position of the destination node
            new_file_name (str): Optional argument. Name of the destination file (None to keep original name)
        """
        dest_folder: str = self.folder_nodes[node_pos].folder_name

        if isinstance(file_source, list):
            for file in file_source:
                dest_sub_folder: str = dest_folder + file[1]
                
                if dest_sub_folder[-1] != "/":
                    dest_sub_folder += "/"
                
                if subprocess.run(["test","-d", dest_sub_folder]).returncode != 0:
                    subprocess.run(["mkdir", dest_sub_folder], stdout=subprocess.DEVNULL)
                
                if not new_file_name is None:
                    dest_sub_folder += new_file_name
                subprocess.run(["cp", file[0], dest_sub_folder], stdout=subprocess.DEVNULL)               
        else:
            if not new_file_name is None:
                dest_folder = self.folder_nodes[node_pos].folder_name + new_file_name
            subprocess.run(["cp", file_source, dest_folder], stdout=subprocess.DEVNULL)   


    def does_node_have_file(self, file_name: str, node_pos: int) -> bool:
        return subprocess.call(["test", "-e", self.folder_nodes[node_pos].folder_name + file_name]) == 0

    def list_existing_files(self, node_pos:int) -> List[str]:
        res: List[str] =  list(filter(lambda x: x.startswith("ERA5-"), 
                            subprocess.check_output(['ls', self.folder_nodes[node_pos].folder_name], shell=False)\
                                      .decode('utf-8')\
                                      .split("\n")))
        res.sort(key=key_function_for_file)
        return res