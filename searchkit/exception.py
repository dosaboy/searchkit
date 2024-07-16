class FileSearchException(Exception):
    """ Exception raised when an error occurs during a file search. """
    def __init__(self, msg):
        self.msg = msg
