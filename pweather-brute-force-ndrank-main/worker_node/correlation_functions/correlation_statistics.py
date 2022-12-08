from typing import Any, Dict

class CorrelationStatistics:
    """Class the contains the gathered statistics 
    from the dataset
    """
    def __init__(self, values: Dict[str, Any]) -> None:
        self._values: Dict[str, Any] = values

    def get_values(self) -> Dict[str,Any]:
        return self._values

    def __str__(self) -> str:
        return str(self._values)