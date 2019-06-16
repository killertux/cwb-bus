from datetime import datetime
from .filetype import FileType


class Downloader:
	
	def __init__(self, date_start: datetime = datetime(2017,1,18), date_end: datetime = datetime.now(), file_type: FileType = FileType.ALL):
		self.file_type = file_type
		self.date_end = date_end
		self.date_start = date_start
