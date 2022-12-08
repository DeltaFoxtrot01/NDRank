

class OutOfSocketPortsError(Exception):
    """Exception thrown in case there is no more available ports 
    to be used by the file protocol

    """
    def __init__(self, *args: object) -> None:
        super().__init__(*args)