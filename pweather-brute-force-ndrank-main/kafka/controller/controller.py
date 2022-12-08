from service.service import ServiceBase


class ControllerBase:
    """Base interface for the protocol layer, the layer responsible for
    reading from the message queue system and publishing the final result
    """

    def __init__(self, service: ServiceBase):
        self._service = service

    

    def start(self) -> None:
        """Kicks off the process of reading from the message queue system"""
        raise NotImplementedError("Method must be overriden")


    def close(self) -> None:
        """Closes all required channels before shutding down the server"""
        pass