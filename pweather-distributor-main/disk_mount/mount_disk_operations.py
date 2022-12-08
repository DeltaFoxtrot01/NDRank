import logging
import subprocess
from subprocess import CompletedProcess, run
from typing import List, Optional
from typing_extensions import final

A_ASCII = 97
START_DISK_PATH = "/dev/sd"
TEMP_FOLDER_PREFIX = "mounted_disk_"

def automated_mount(num_disks: int, working_folder: str, run_with_sudo: bool = True, skip_formatting: bool = False) -> List[str]:
    """
    Automatically mounts disks into a created folder.

    To define the paths of where to look for the disks, the path in START_DISK_PATH
    is used and start in 'b'

    Args:
        num_disks (int): number of disks
        working_folder (str): path of the folder
        run_with_sudo (bool): add sudo to the commands that require sudo (true as default)
        skip_formatting (bool): if disks should be formated or not

    Returns:
        List[str]: path for folders with mounted disks
    """

    if working_folder[-1] != "/":
        working_folder += "/"

    disks: List[str] = []
    temp_folders: List[str] = []

    # step one: define the volume paths
    for i in range(1,num_disks+1):
        disks.append(START_DISK_PATH + chr(A_ASCII + i))

    # step two: create temporary folders
    for i in range(num_disks):
        temp_folder: str = working_folder + TEMP_FOLDER_PREFIX + str(i)
        
        final_state: CompletedProcess = subprocess.run(["test", "-d",temp_folder])
        if final_state.returncode != 0:
            logging.debug("Creating folder " + temp_folder)
            final_state: CompletedProcess = subprocess.run(["mkdir", temp_folder])
            final_state.check_returncode()
        else:
            logging.debug("Folder " + temp_folder + " already exists")
        temp_folders.append(temp_folder)
    
    # step three: format, mount disks and change permissions of folder
    for i in range(num_disks):
        #verify if folder already has a mounted disk
        final_state: CompletedProcess = subprocess.run(["mountpoint", "-q", temp_folders[i]])
        if final_state.returncode == 1:
            logging.debug("Mounting volume " + disks[i] + " in folder " + temp_folders[i])
            #if not format with a ext4 file system
            final_state: Optional[CompletedProcess] = None
            if not skip_formatting:
                if run_with_sudo:
                    final_state: CompletedProcess = subprocess.run([
                                    "sudo", "mkfs.ext4", "-m", "0", "-F", "-E",
                                    "lazy_itable_init=0,lazy_journal_init=0,discard",
                                    disks[i]])
                else:
                    final_state: CompletedProcess = subprocess.run([
                                    "mkfs.ext4", "-m", "0", "-F", "-E",
                                    "lazy_itable_init=0,lazy_journal_init=0,discard",
                                    disks[i]])
            
                final_state.check_returncode()
            #mount device in the folder
            final_state = None
            if run_with_sudo:
                final_state = subprocess.run(["sudo", "mount", "-o","discard,defaults",
                                            disks[i], temp_folders[i]])
            else:
                final_state = subprocess.run(["mount", "-o","discard,defaults",
                                            disks[i], temp_folders[i]])
            final_state.check_returncode()
        else:
            logging.debug("Folder " + temp_folders[i] + " is already a mounting point")

        #set the required permissions
        final_state = None
        if run_with_sudo:
            final_state = subprocess.run(["sudo", "chmod", "777", temp_folders[i]])
        else:
            final_state = subprocess.run(["chmod", "777", temp_folders[i]])
        
        final_state.check_returncode()

    return temp_folders

    

def automated_umount(mounted_folders: List[str], run_with_sudo: bool = True) -> None:
    """
    Automatically unmounts existing mounted folders

    Args:
        mounted_folders (List[str]): folders with mounted disks
        run_with_sudo (bool): add sudo to the commands that require sudo (true as default)
    """
    for folder in mounted_folders:
        final_state: CompletedProcess 
        if run_with_sudo:
            final_state = subprocess.run(["sudo", "umount", folder])
        else:
            final_state = subprocess.run(["umount", folder])
        final_state.check_returncode()
        final_state: CompletedProcess = subprocess.run(["rm", "-r", folder])
        final_state.check_returncode()
        