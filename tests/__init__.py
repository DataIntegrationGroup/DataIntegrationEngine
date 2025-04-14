def recursively_clean_directory(path):
    """Recursively delete all files and directories in the given path."""
    for item in path.iterdir():
        if item.is_dir():
            recursively_clean_directory(item)
        else:
            item.unlink()
    path.rmdir()