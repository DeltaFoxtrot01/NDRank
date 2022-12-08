import importlib
from importlib import resources
import logging
from typing import Iterator
from auxiliar import component_injector

logger:logging.Logger = logging.getLogger("__main__")


def _automatic_import(package: str) -> None:
    files: Iterator[str] = resources.contents(package)

    py_files: Iterator[str] = \
        map(lambda x: x.split(".py")[0],
            filter(
                lambda x: x.endswith(".py") and x[0] != "_",
                files
            )
        )
    
    for py_file in py_files:
        logger.info(py_file, package)
        importlib.import_module(package + "." + py_file)


_automatic_import("repository.implementations")
_automatic_import("service.implementations")
_automatic_import("correlation_functions.implementations")
