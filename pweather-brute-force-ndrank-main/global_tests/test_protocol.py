import logging
import signal
import subprocess
import time
from typing import Final, Iterator, List
import pytest

PATH_OF_INPUT_FILES: Final = "./global_tests/input_files/"
PATH_OF_TEMP_FILES: Final = "./global_tests/temporary_folder/"
PATH_OF_RESULT_FILES: Final = "./global_tests/results_folder/"
PATH_TO_PROPERTIES_FILES: Final = "./global_tests/protocol_properties_files/"
WORKER_PATH: Final = "./worker_node/"
MASTER_PATH: Final = "./master_node/"


def get_files_from_folder(path: str) -> List[str]:
    return list(filter(lambda file: not file in ["", ".gitignore"] , 
                        subprocess.check_output(['ls', path], shell=False)\
                                  .decode('utf-8')\
                                  .split("\n")))                     

def cleanup() -> None:
    list_of_files: List[str] = list(map(lambda path: PATH_OF_RESULT_FILES + path, get_files_from_folder(PATH_OF_RESULT_FILES)))
    list_of_files += list(map(lambda path: PATH_OF_TEMP_FILES + path, get_files_from_folder(PATH_OF_TEMP_FILES)))

    for file in list_of_files:
        subprocess.run(["rm", file])

@pytest.fixture(autouse=True)
def cleanup_routine() -> Iterator:

    yield
    cleanup()


def test_file_tranfer() -> None:
    """
    The main idea of this test is to evaluate the file transfer component of the 
    protocol and verify if the given files are created correctlys
    """
    input_files: List[str] = ["input1","input2","input3"]
    p1: subprocess.Popen = subprocess.Popen(["python3", 
                                            WORKER_PATH + "__main__.py", "-d", "false",
                                            "-p", PATH_TO_PROPERTIES_FILES + "worker_properties.yaml"])
    time.sleep(1)
    p2 = subprocess.run(["python3", 
                         MASTER_PATH + "__main__.py", 
                         "-p", PATH_TO_PROPERTIES_FILES + "master_properties.yaml",
                         "-r", PATH_TO_PROPERTIES_FILES + "requests.yaml"])
                           
    p1.send_signal(signal.SIGINT)
    p1.wait()

    assert p1.returncode == 0
    assert p2.returncode == 0

    print("Existing files in temporary folder: " + str(get_files_from_folder(PATH_OF_TEMP_FILES)))
    found_file: bool = False
    for input_file in input_files:
        found_file = False
        for temp_file in get_files_from_folder(PATH_OF_TEMP_FILES):
            if temp_file.endswith(input_file):
                found_file = True
                assert subprocess.run(["cmp", 
                                       "--silent", 
                                       PATH_OF_INPUT_FILES + input_file,
                                       PATH_OF_TEMP_FILES + temp_file]).returncode == 0
                break
        assert found_file

"""
    - settings file not found
    - file indexed in settings file not found
    - path not found
    - file is not in .grib or .nc format
    - input is not in .grib or .nc format
    - problem opening socket port
"""

def test_global_unable_find_dataset_files() -> None:
    p = subprocess.run(["python3", 
                    WORKER_PATH + "__main__.py", 
                    "-p", PATH_TO_PROPERTIES_FILES + 
                    "worker_properties_for_non_existing_dataset.yaml"])
    assert p.returncode != 0