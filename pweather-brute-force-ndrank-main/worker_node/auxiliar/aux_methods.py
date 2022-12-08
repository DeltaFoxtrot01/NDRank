

def fix_path(path: str) -> str:
    """Verifies if the path ends with a "/",
    returning the path in a correct format

    Args:
        path (str): path to be fixed

    Returns:
        str: path with a "/" in the end
    """

    if path[-1] == "/":
        return path
    else:
        return path + "/"