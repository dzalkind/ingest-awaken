import os


def expand(relpath: str, invocation_file: str) -> str:
    cwd = os.path.dirname(invocation_file)
    path = os.path.join(cwd, relpath)
    return os.path.realpath(path)
