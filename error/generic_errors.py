class GenericError(Exception):
	pass

class FileNotFoundError(GenericError):
	pass

class FileOpenError(GenericError):
	pass