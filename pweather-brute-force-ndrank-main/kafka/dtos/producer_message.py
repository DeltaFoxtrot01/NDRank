from typing import Any, Dict, List


class ProducerMessage:
    """Dto of the message that is going to be submitted to the queue"""
    def __init__(self, request_id: str, values: List[Dict[str,Any]]):
        """Basic constructor

        Args:
            request_id (str): id of the request that should be published as a topic
            values (List[Dict[str,Any]]): values to be submitted
        """
        self._values: List[Dict[str,Any]] = values
        self._request_id: str = request_id

    @property
    def values(self) -> List[Dict[str,Any]]:
        return self._values

    @property
    def request_id(self) -> str:
        return self._request_id

    def __str__(self) -> str:
        return "request_id: " + self._request_id + " " + str(self._values)
