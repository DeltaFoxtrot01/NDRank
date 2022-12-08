from datetime import datetime
from typing import Dict, Final, List, Set, Optional

import numpy as np
import pandas
from repository.auxiliary_structures.constants import ALL, HOUR


class TimeGapContainer:
    """Manages the existing gaps in the time dimension for
    each data variable"""

    def __init__(self) -> None:
        self._valid_tags: Set[str] = set([HOUR])
        self._gap_for_all_vars: Dict[str,Set[int]] = {}
        self._gap_for_all_vars[HOUR] = set()

        #keys are organized as follows: (data_var, time tag)
        self._gap_for_specific_vars: Dict[str,Dict[str,Set[int]]] = {}

    def add_time_gap(self, tag: str, time_value: int, data_var: str) -> None:
        """Adds a time value for a specific hour or day (for example) where there
        is no available data for the given data variables

        Args:
            tag (str): tag describing what kind of time instance it reffers to (if it is the hour, day, etc.)
            time_value (int): the value for the time instance
            data_var (str): the specific data var it applies to or ALL if it reffers to all data variables

        Raises:
            ValueError: in case the tag is not valid
        """
        if not tag in self._valid_tags:
            raise ValueError("Tag " + tag + " does not exist in TimeGapContainer")
        if data_var == ALL:
            self._gap_for_all_vars[tag].add(time_value)
        else:
            if not data_var in self._gap_for_specific_vars:
                self._gap_for_specific_vars[data_var] = {}
            
            if not tag in self._gap_for_specific_vars[data_var]:
                self._gap_for_specific_vars[data_var][tag] = set()

            self._gap_for_specific_vars[data_var][tag].add(time_value)

    
    def _is_gap_hour(self, ts: np.datetime64, data_vars: List[str]) -> bool:
        ts_datetime: datetime = pandas.to_datetime(ts)
        for var in data_vars:
            if ts_datetime.hour in self._gap_for_all_vars[HOUR]:
                return True
            if var in self._gap_for_specific_vars and \
                ts_datetime.hour in self._gap_for_specific_vars[var][HOUR]:
                return True
        return False
    
    def _is_search_hour(self, ts:np.datetime64, search_hours: List[int]) -> bool:
        """Verifies if the hour exists inside the search hours

        Args:
            ts (np.datetime64): given timestamp
            search_hours (List[int]): hours that should be searched

        Returns:
            bool: True if the hour is inside search_hours
        """
        ts_datetime: datetime = pandas.to_datetime(ts)
        return ts_datetime.hour in search_hours

    def is_gap(self, ts: np.datetime64, data_vars: List[str], search_hours: Optional[List[int]] = None) -> bool:
        """True if the given timestamp is a time instance value where
        there is a gap in information for the given data variable

        Args:
            ts (np.datetime64): time instance to be analysed
            data_vars (List[str]): data variables to be checked
            search_hours (Optional[List[int]]): used in a scenario where 
            the search should be executed in a specific set of hours, if 
            this option should not be used, it can be set to None. Default
            is None.

        Returns:
            bool: true if it is in the gap
        """
        if search_hours is None:
            return self._is_gap_hour(ts,data_vars)
        else:
            if self._is_search_hour(ts,search_hours):
                return self._is_gap_hour(ts,data_vars)
            else:
                return True
