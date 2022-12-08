from typing import List, Tuple
import pytest

from repository.auxiliary_structures.dataset_indexer import MONTH_YEAR_DATASET, PROCESSING_FUNCTIONS, DatasetIndexer, DateContainer


def test_date_container() -> None:
    assert 1980 == hash(DateContainer(1980))
    assert 198010 == hash(DateContainer(1980,10))
    assert 11980212 == hash(DateContainer(1198,2,12))
    assert 11981212 == hash(DateContainer(1198,12,12))
    assert 1198121201 == hash(DateContainer(1198,12,12,1))
    assert 198121201 == hash(DateContainer(198,12,12,1))


def test_valid_file_develop_name() -> None:
    assert 198012 == PROCESSING_FUNCTIONS[MONTH_YEAR_DATASET]("ERA5-12-1980.nc")[0]
    assert 200002 == PROCESSING_FUNCTIONS[MONTH_YEAR_DATASET]("ERA5-2-2000.nc")[0]

    with pytest.raises(ValueError) as exec_info:
        PROCESSING_FUNCTIONS[MONTH_YEAR_DATASET]("ERA-12-19.nc")
    
    with pytest.raises(ValueError) as exec_info:
        PROCESSING_FUNCTIONS[MONTH_YEAR_DATASET]("something")

    with pytest.raises(ValueError) as exec_info:
        PROCESSING_FUNCTIONS[MONTH_YEAR_DATASET]("ERA_12-19.nc")
    
    with pytest.raises(ValueError) as exec_info:
        PROCESSING_FUNCTIONS[MONTH_YEAR_DATASET]("ERA-12_19.nc")
    
    
def test_dataset_index_correctness() -> None:
    comparison_result: List[Tuple[int,str]] = [
        (198001,"./worker_node/testing_dataset/ERA5-1-1980.nc"),
        (198002,"./worker_node/testing_dataset/ERA5-2-1980.nc"),
        (198003,"./worker_node/testing_dataset/ERA5-3-1980.nc")
    ]
    indexer: DatasetIndexer = \
        DatasetIndexer("./worker_node/testing_dataset/settings.yaml",
                       "./worker_node/testing_dataset/",
                       PROCESSING_FUNCTIONS[MONTH_YEAR_DATASET])

    res: List[Tuple[int,str,DateContainer]] = indexer.get_sorted_file_paths()
    for elem in range(len(res)):
        assert res[elem][0] == comparison_result[elem][0]
        assert res[elem][1] == comparison_result[elem][1]


def test_dataset_index_sort() -> None:
    comparison_result: List[Tuple[int,str]] = [
        (198001,"./worker_node/testing_dataset_for_indexer/dataset1/ERA5-1-1980.nc"),
        (198002,"./worker_node/testing_dataset_for_indexer/dataset1/ERA5-2-1980.nc"),
        (198010,"./worker_node/testing_dataset_for_indexer/dataset1/ERA5-10-1980.nc"),
        (198101,"./worker_node/testing_dataset_for_indexer/dataset1/ERA5-1-1981.nc"),
        (198110,"./worker_node/testing_dataset_for_indexer/dataset1/ERA5-10-1981.nc")
    ]
    indexer: DatasetIndexer = \
        DatasetIndexer("./worker_node/testing_dataset_for_indexer/dataset1/settings.yaml",
                       "./worker_node/testing_dataset_for_indexer/dataset1/",
                       PROCESSING_FUNCTIONS[MONTH_YEAR_DATASET])

    res: List[Tuple[int,str,DateContainer]] = indexer.get_sorted_file_paths()
    for elem in range(len(res)):
        assert res[elem][0] == comparison_result[elem][0]
        assert res[elem][1] == comparison_result[elem][1]

def test_dataset_index_with_invalid_file_names() -> None:
    with pytest.raises(ValueError) as exec_info:
        indexer: DatasetIndexer = \
            DatasetIndexer("./worker_node/testing_dataset_for_indexer/dataset2/settings.yaml",
                           "./worker_node/testing_dataset_for_indexer/dataset2/",
                           PROCESSING_FUNCTIONS[MONTH_YEAR_DATASET])
