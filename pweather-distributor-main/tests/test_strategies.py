import pytest

from typing import List
from typing_extensions import Final
from distributor.distributor import distributor
from downloader.downloader import downloader_interface, mock_downloader
from uploader.uploader import mock_uploader, uploader_interface

METADATA_ATTRS: Final = {
    "time-variation-dim": "step",
    "time-initial-dim": "time",
    "resolution-reduction-parameters": {},
    "step": 10.0
}


def test_round_robin_strategy():
    num_nodes: int = 4
    strategy: str = "round-robin-mock"
    metadata_strategy: str = "dummy"

    downloader: downloader_interface = None
    uploader: uploader_interface = None

    downloader = mock_downloader("/test/dev_source_files",100)
    uploader = mock_uploader(num_nodes)

    distributor_instance: distributor = distributor(downloader,uploader,"./temp_download_folder",METADATA_ATTRS)
    elems = distributor_instance.download_and_upload(strategy,metadata_strategy)

    original_files: List[str] = downloader.get_object_list()

    total_size = 0

    for elem in elems:
        total_size += len(elem)

    assert total_size == len(original_files)

    node_index = 0

    for file in original_files:
        assert file in elems[node_index]

        if node_index >= num_nodes -1:
            node_index = 0
        else:
            node_index += 1


def test_round_time_interval_reduced():
    num_nodes: int = 4
    strategy: str = "time-interval-mock"
    metadata_strategy: str = "dummy"

    base_num_of_elems: int = 100

    for i in range(num_nodes):
        downloader: downloader_interface = None
        uploader: uploader_interface = None

        downloader = mock_downloader("/test/dev_source_files",base_num_of_elems + i)
        uploader = mock_uploader(num_nodes)

        distributor_instance: distributor = distributor(downloader,uploader,"./temp_download_folder",METADATA_ATTRS)
        elems = distributor_instance.download_and_upload(strategy,metadata_strategy)

        original_files: List[str] = downloader.get_object_list()

        total_size = 0

        for elem in elems:
            total_size += len(elem)

        assert total_size == len(original_files)

        for node_list in elems:
            assert sorted(node_list,key=lambda x: int(x.split("ERA5-1-")[-1].split(".")[0]))

        for i in range(len(elems)):
            for j in range(len(elems)):
                if i == j:
                    continue
                for elem in elems[i]:
                    assert not elem in elems[j]
    