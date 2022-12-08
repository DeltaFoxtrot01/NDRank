from typing import List, Tuple
import pytest
import numpy as np
from correlation_functions.main_structure import CorrelationFunction
from service.data_types import CandidateContainer, CandidateListManager

class TemporaryCorrFunc(CorrelationFunction):

    def compare(self, value1: float, value2: float) -> bool:
        return value1 > value2

def test_empty_list_of_candidates() -> None:
    manager: CandidateListManager = \
        CandidateListManager(TemporaryCorrFunc("test"),5)

    res: List[np.datetime64] = []
    
    for elem in manager.get_results():
        res.append(elem)
    assert len(res) == 0

def test_smaller_than_top_n() -> None:
    manager: CandidateListManager = \
        CandidateListManager(TemporaryCorrFunc("test"),15)
    list_input: List[Tuple[np.datetime64, CandidateContainer]] = \
    [
        (np.datetime64('2005-02-11'), CandidateContainer(1, 1, ["x"])),
        (np.datetime64('2005-02-10'), CandidateContainer(2, 4, ["x"])),
        (np.datetime64('2005-02-09'), CandidateContainer(1, 2, ["x"])),
        (np.datetime64('2005-02-08'), CandidateContainer(2, 3, ["x"])),
        (np.datetime64('2005-02-07'), CandidateContainer(6, 8, ["x"])),
        (np.datetime64('2005-02-06'), CandidateContainer(2, 10, ["x"])),
        (np.datetime64('2005-02-05'), CandidateContainer(3, 5, ["x"])),
        (np.datetime64('2005-02-04'), CandidateContainer(2, 32, ["x"])),
        (np.datetime64('2005-02-03'), CandidateContainer(4, 41, ["x"]))
    ]
    final_result: List[np.datetime64] = [
        np.datetime64('2005-02-11'),
        np.datetime64('2005-02-09'),
        np.datetime64('2005-02-08'),
        np.datetime64('2005-02-10'),
        np.datetime64('2005-02-05'),
        np.datetime64('2005-02-07'),
        np.datetime64('2005-02-06'),
        np.datetime64('2005-02-04'),
        np.datetime64('2005-02-03')        
    ]
    
    for elem in list_input:
        manager.add_value(elem[0],elem[1])

    index: int = 0
    for ts in manager.get_results():
        assert ts == final_result[index]
        index += 1


def test_list_equal_size() -> None:
    manager: CandidateListManager = \
        CandidateListManager(TemporaryCorrFunc("test"),10)
    list_input: List[Tuple[np.datetime64, CandidateContainer]] = \
    [
        (np.datetime64('2005-02-11'), CandidateContainer(1, 1, ["x"])),
        (np.datetime64('2005-02-10'), CandidateContainer(2, 4, ["x"])),
        (np.datetime64('2005-02-09'), CandidateContainer(1, 2, ["x"])),
        (np.datetime64('2005-02-08'), CandidateContainer(2, 3, ["x"])),
        (np.datetime64('2005-02-07'), CandidateContainer(6, 8, ["x"])),
        (np.datetime64('2005-02-06'), CandidateContainer(2, 10, ["x"])),
        (np.datetime64('2005-02-05'), CandidateContainer(3, 5, ["x"])),
        (np.datetime64('2005-02-04'), CandidateContainer(2, 32, ["x"])),
        (np.datetime64('2005-02-03'), CandidateContainer(4, 41, ["x"])),
        (np.datetime64('2005-03-09'), CandidateContainer(1, 40, ["x"]))
    ]
    final_result: List[np.datetime64] = [
        np.datetime64('2005-02-11'),
        np.datetime64('2005-02-09'),
        np.datetime64('2005-02-08'),
        np.datetime64('2005-02-10'),
        np.datetime64('2005-02-05'),
        np.datetime64('2005-02-07'),
        np.datetime64('2005-02-06'),
        np.datetime64('2005-02-04'),
        np.datetime64('2005-03-09'),
        np.datetime64('2005-02-03')
    ]
    
    for elem in list_input:
        manager.add_value(elem[0],elem[1])

    index: int = 0
    for ts in manager.get_results():
        assert ts == final_result[index]
        index += 1

def test_list_with_equal_inputs() -> None:
    manager: CandidateListManager = \
        CandidateListManager(TemporaryCorrFunc("test"),10)
    list_input: List[Tuple[np.datetime64, CandidateContainer]] = \
    [
        (np.datetime64('2005-02-11'), CandidateContainer(1, 1, ["x"])),
        (np.datetime64('2005-02-11'), CandidateContainer(1, 1, ["x"])),
        (np.datetime64('2005-02-11'), CandidateContainer(1, 1, ["x"])),
        (np.datetime64('2005-02-11'), CandidateContainer(1, 1, ["x"])),
        (np.datetime64('2005-02-11'), CandidateContainer(1, 1, ["x"])),
        (np.datetime64('2005-02-11'), CandidateContainer(1, 1, ["x"])),
        (np.datetime64('2005-02-11'), CandidateContainer(1, 1, ["x"])),
        (np.datetime64('2005-02-11'), CandidateContainer(1, 1, ["x"])),
        (np.datetime64('2005-02-11'), CandidateContainer(1, 1, ["x"])),
        (np.datetime64('2005-02-11'), CandidateContainer(1, 1, ["x"])),
        (np.datetime64('2005-02-11'), CandidateContainer(1, 1, ["x"])),
        (np.datetime64('2005-02-11'), CandidateContainer(1, 1, ["x"]))
    ]
    
    for elem in list_input:
        manager.add_value(elem[0],elem[1])

    res: List[np.datetime64] = []

    for ts in manager.get_results():
        assert ts == np.datetime64('2005-02-11')
        res.append(ts)

    assert len(res) == len(list_input)

def test_basic_list_candidates() -> None:
    manager: CandidateListManager = \
        CandidateListManager(TemporaryCorrFunc("test"),10)
    list_input: List[Tuple[np.datetime64, CandidateContainer]] = \
    [
        (np.datetime64('2005-02-11'), CandidateContainer(19, 40, ["x"])),
        (np.datetime64('2005-02-10'), CandidateContainer(12, 41, ["x"])),
        (np.datetime64('2005-02-09'), CandidateContainer(9, 56, ["x"])),
        (np.datetime64('2005-02-08'), CandidateContainer(12, 35, ["x"])),
        (np.datetime64('2005-02-07'), CandidateContainer(11, 46, ["x"])),
        (np.datetime64('2005-02-06'), CandidateContainer(4, 23, ["x"])),
        (np.datetime64('2005-02-05'), CandidateContainer(90, 400, ["x"])),
        (np.datetime64('2005-02-04'), CandidateContainer(23, 49, ["x"])),
        (np.datetime64('2005-02-03'), CandidateContainer(12, 14, ["x"])),
        (np.datetime64('2000-04-02'), CandidateContainer(7, 29, ["x"])),
        (np.datetime64('2000-02-01'), CandidateContainer(1, 4, ["x"])),
        (np.datetime64('2005-04-01'), CandidateContainer(1, 3, ["x"])),
        (np.datetime64('2005-02-01'), CandidateContainer(2, 4, ["x"])),
        (np.datetime64('2005-03-01'), CandidateContainer(2, 3, ["x"])),
        (np.datetime64('2006-02-01'), CandidateContainer(6, 7, ["x"])),
        (np.datetime64('1980-02-01'), CandidateContainer(1, 19, ["x"])),
        (np.datetime64('2001-06-01'), CandidateContainer(18, 20, ["x"])),
        (np.datetime64('2003-10-01'), CandidateContainer(5, 32, ["x"])),
        (np.datetime64('2002-02-04'), CandidateContainer(14, 28, ["x"]))
    ]

    final_result: List[Tuple[np.datetime64, CandidateContainer]] = [
        (np.datetime64('2005-04-01'), CandidateContainer(1, 3, ["x"])),
        (np.datetime64('2005-03-01'), CandidateContainer(2, 3, ["x"])),
        (np.datetime64('2000-02-01'), CandidateContainer(1, 4, ["x"])),
        (np.datetime64('2005-02-01'), CandidateContainer(2, 4, ["x"])),
        (np.datetime64('2006-02-01'), CandidateContainer(6, 7, ["x"])),
        (np.datetime64('2005-02-03'), CandidateContainer(12, 14, ["x"])),
        (np.datetime64('1980-02-01'), CandidateContainer(1, 19, ["x"])),
        (np.datetime64('2001-06-01'), CandidateContainer(18, 20, ["x"])),
        (np.datetime64('2005-02-06'), CandidateContainer(4, 23, ["x"])),
        (np.datetime64('2002-02-04'), CandidateContainer(14, 28, ["x"])),
        (np.datetime64('2000-04-02'), CandidateContainer(7, 29, ["x"])),
        (np.datetime64('2003-10-01'), CandidateContainer(5, 32, ["x"])),
        (np.datetime64('2005-02-08'), CandidateContainer(12, 35, ["x"])),
        (np.datetime64('2005-02-11'), CandidateContainer(19, 40, ["x"])),
        (np.datetime64('2005-02-10'), CandidateContainer(12, 41, ["x"])),
        (np.datetime64('2005-02-07'), CandidateContainer(11, 46, ["x"])),
        (np.datetime64('2005-02-04'), CandidateContainer(23, 49, ["x"])),
        (np.datetime64('2005-02-09'), CandidateContainer(9, 56, ["x"]))
    ]

    for elem in list_input:
        manager.add_value(elem[0],elem[1])

    res: List[np.datetime64] = []
    for ts in manager.get_results():
        res.append(ts)

    assert len(res) == len(final_result)
    
    for i in range(len(res)):
        assert res[i] == final_result[i][0]


def test_list_with_all_elements_with_better_value_than_the_worst_in_the_top_n() -> None:
    manager: CandidateListManager = \
        CandidateListManager(TemporaryCorrFunc("test"),10)
    list_input: List[Tuple[np.datetime64, CandidateContainer]] = \
    [
        (np.datetime64('2005-02-01'), CandidateContainer(4, 23, ["x"])),
        (np.datetime64('2000-02-01'), CandidateContainer(3, 22, ["x"])),
        (np.datetime64('2005-02-04'), CandidateContainer(17, 36, ["x"])),
        (np.datetime64('2005-02-03'), CandidateContainer(6, 25, ["x"])),
        (np.datetime64('2005-04-01'), CandidateContainer(1, 20, ["x"])),
        (np.datetime64('2006-02-01'), CandidateContainer(5, 24, ["x"])),
        (np.datetime64('2000-04-02'), CandidateContainer(11, 30, ["x"])),
        (np.datetime64('1980-02-01'), CandidateContainer(7, 26, ["x"])),
        (np.datetime64('2005-03-01'), CandidateContainer(2, 21, ["x"])),
        (np.datetime64('2005-02-09'), CandidateContainer(18, 37, ["x"])),
        (np.datetime64('2005-02-06'), CandidateContainer(9, 28, ["x"])),
        (np.datetime64('2002-02-04'), CandidateContainer(10, 29, ["x"])),
        (np.datetime64('2003-10-01'), CandidateContainer(12, 31, ["x"])),
        (np.datetime64('2001-06-01'), CandidateContainer(8, 27, ["x"])),
        (np.datetime64('2005-02-07'), CandidateContainer(16, 35, ["x"])),
        (np.datetime64('2005-02-08'), CandidateContainer(13, 32, ["x"])),
        (np.datetime64('2005-02-11'), CandidateContainer(14, 33, ["x"])),
        (np.datetime64('2005-02-10'), CandidateContainer(15, 34, ["x"]))
    ]

    final_result: List[Tuple[np.datetime64, CandidateContainer]] = [
        (np.datetime64('2005-04-01'), CandidateContainer(1, 20, ["x"])),
        (np.datetime64('2005-03-01'), CandidateContainer(2, 21, ["x"])),
        (np.datetime64('2000-02-01'), CandidateContainer(3, 22, ["x"])),
        (np.datetime64('2005-02-01'), CandidateContainer(4, 23, ["x"])),
        (np.datetime64('2006-02-01'), CandidateContainer(5, 24, ["x"])),
        (np.datetime64('2005-02-03'), CandidateContainer(6, 25, ["x"])),
        (np.datetime64('1980-02-01'), CandidateContainer(7, 26, ["x"])),
        (np.datetime64('2001-06-01'), CandidateContainer(8, 27, ["x"])),
        (np.datetime64('2005-02-06'), CandidateContainer(9, 28, ["x"])),
        (np.datetime64('2002-02-04'), CandidateContainer(10, 29, ["x"])),
        (np.datetime64('2000-04-02'), CandidateContainer(11, 30, ["x"])),
        (np.datetime64('2003-10-01'), CandidateContainer(12, 31, ["x"])),
        (np.datetime64('2005-02-08'), CandidateContainer(13, 32, ["x"])),
        (np.datetime64('2005-02-11'), CandidateContainer(14, 33, ["x"])),
        (np.datetime64('2005-02-10'), CandidateContainer(15, 34, ["x"])),
        (np.datetime64('2005-02-07'), CandidateContainer(16, 35, ["x"])),
        (np.datetime64('2005-02-04'), CandidateContainer(17, 36, ["x"])),
        (np.datetime64('2005-02-09'), CandidateContainer(18, 37, ["x"]))
    ]

    for elem in list_input:
        manager.add_value(elem[0],elem[1])

    res: List[np.datetime64] = []
    for ts in manager.get_results():
        res.append(ts)

    assert len(res) == len(final_result)
    
    for i in range(len(res)):
        assert res[i] == final_result[i][0]


def test_with_elements_quite_far_away() -> None:
    manager: CandidateListManager = \
        CandidateListManager(TemporaryCorrFunc("test"),9)
    list_input: List[Tuple[np.datetime64, CandidateContainer]] = \
    [
        (np.datetime64('2005-02-11'), CandidateContainer(140, 330, ["x"])),
        (np.datetime64('2002-02-04'), CandidateContainer(100, 290, ["x"])),
        (np.datetime64('2005-02-08'), CandidateContainer(130, 320, ["x"])),
        (np.datetime64('2005-02-09'), CandidateContainer(180, 370, ["x"])),
        (np.datetime64('2000-04-02'), CandidateContainer(110, 300, ["x"])),
        (np.datetime64('2005-02-04'), CandidateContainer(170, 360, ["x"])),
        (np.datetime64('2005-02-10'), CandidateContainer(150, 340, ["x"])),
        (np.datetime64('2005-02-07'), CandidateContainer(160, 350, ["x"])),
        (np.datetime64('2003-10-01'), CandidateContainer(120, 310, ["x"])),
        (np.datetime64('2005-02-01'), CandidateContainer(4, 23, ["x"])),
        (np.datetime64('2005-04-01'), CandidateContainer(1, 20, ["x"])),
        (np.datetime64('2005-02-03'), CandidateContainer(6, 25, ["x"])),
        (np.datetime64('2006-02-01'), CandidateContainer(5, 24, ["x"])),
        (np.datetime64('2005-03-01'), CandidateContainer(2, 21, ["x"])),
        (np.datetime64('2005-02-06'), CandidateContainer(9, 28, ["x"])),
        (np.datetime64('1980-02-01'), CandidateContainer(7, 26, ["x"])),
        (np.datetime64('2000-02-01'), CandidateContainer(3, 22, ["x"])),
        (np.datetime64('2001-06-01'), CandidateContainer(8, 27, ["x"]))
    ]

    final_result: List[Tuple[np.datetime64, CandidateContainer]] = [
        (np.datetime64('2005-04-01'), CandidateContainer(1, 20, ["x"])),
        (np.datetime64('2005-03-01'), CandidateContainer(2, 21, ["x"])),
        (np.datetime64('2000-02-01'), CandidateContainer(3, 22, ["x"])),
        (np.datetime64('2005-02-01'), CandidateContainer(4, 23, ["x"])),
        (np.datetime64('2006-02-01'), CandidateContainer(5, 24, ["x"])),
        (np.datetime64('2005-02-03'), CandidateContainer(6, 25, ["x"])),
        (np.datetime64('1980-02-01'), CandidateContainer(7, 26, ["x"])),
        (np.datetime64('2001-06-01'), CandidateContainer(8, 27, ["x"])),
        (np.datetime64('2005-02-06'), CandidateContainer(9, 28, ["x"]))
    ]
    
    for elem in list_input:
        manager.add_value(elem[0],elem[1])

    res: List[np.datetime64] = []
    for ts in manager.get_results():
        res.append(ts)

    assert len(res) == len(final_result)
    
    for i in range(len(res)):
        assert res[i] == final_result[i][0]