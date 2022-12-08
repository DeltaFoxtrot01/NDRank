from typing import List, Tuple
import pytest
import numpy as np

from repository.auxiliary_structures.time_gap_container import TimeGapContainer

def test_dates_with_time_gap() -> None:
    container: TimeGapContainer = TimeGapContainer()

    container.add_time_gap("hour", 4,"ALL")
    container.add_time_gap("hour", 3,"sd")
    container.add_time_gap("hour", 2,"stm")

    assert container.is_gap(np.datetime64('2005-02-25T03:00'),["sd"])
    assert not container.is_gap(np.datetime64('2005-02-25T03:00'),["stm"])
    
    assert not container.is_gap(np.datetime64('2005-02-25T02:00'),["sd"])
    assert container.is_gap(np.datetime64('2005-02-25T04:00'),["stm"])
    assert container.is_gap(np.datetime64('2005-02-25T04:00'),["sd"])
    
    assert not container.is_gap(np.datetime64('2005-03-25T02:00'),["sd"])
    assert container.is_gap(np.datetime64('2005-03-25T02:00'),["stm"])

    assert container.is_gap(np.datetime64('2005-03-25T02:00'),["stm","sd"])
    assert container.is_gap(np.datetime64('2005-03-25T03:00'),["stm","sd"])
    assert container.is_gap(np.datetime64('2005-03-25T04:00'),["stm","sd"])
    assert not container.is_gap(np.datetime64('2005-03-25T01:00'),["stm","sd"])

    assert not container.is_gap(np.datetime64('2005-03-03T02:00'),["sd"],[2,0])
    assert container.is_gap(np.datetime64('2005-03-03T03:00'),["sd"],[2,0])
    assert container.is_gap(np.datetime64('2005-03-03T03:00'),["stm"],[2,0])
    
    
def test_dates_with_time_gap_with_invalid_args() -> None:
    container: TimeGapContainer = TimeGapContainer()

    with pytest.raises(ValueError):
        container.add_time_gap("lakfsj",2,"ldskj")

    
