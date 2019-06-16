from datetime import datetime
from .filetype import FileType


class Downloader:
	
	def __init__(self, date_start: datetime, date_end: datetime, file_type: FileType):
		self.file_type = file_type
		self.date_end = date_end
		self.date_start = date_start
