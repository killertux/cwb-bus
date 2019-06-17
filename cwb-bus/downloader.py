import asyncio
import aiohttp 
from datetime import date
from filetype import FileType


_session = aiohttp.ClientSession()
_download_folder = "./cache/downloads/"


async def _download_file(data_date: date, session: aiohttp.ClientSession, data_type: FileType = None,
                         base_url: str = "http://dadosabertos.c3sl.ufpr.br/curitibaurbs/") -> bytes:
	"""Internal function that downloads the data.

	This function will try to grab the data from the server using asynchronous requests.
	Will try to grab from the `base_url` plus the types specified and append an extension.
	It will first try the `.xz` extension as it is more common. However, it will also try the `.tar.gz`
	extension as it was also used early on.

	:param data_date: A :class:`datetime.date` instance with the specific date for the data that wants to be gathered
	:param session: A :class:`aiohttp.ClientSession` session for downloading. Should probably only instance 1 for an entire operation
	:param data_type: What type of data to download. If set to `None`, will download all types. See :class:`FileType` for available types
	:param base_url: The base url for downloading data. Shouldn't need to be changed ever
	:return: a `bytes` compressed object, which needs to be decompressed. See :function:`_uncompress_file`
	"""
	pass


async def _uncompress_file(file: bytes) -> bytes:
	"""

	:param file:
	:return:
	"""
	pass


async def get_data(data_date: date, data_type: FileType = None, from_folder: str = None):
	"""

	:param from_folder:
	:param date:
	:param data_type:
	:return:
	"""
	pass

async def get_data_range(start_date: date, end_date: date, data_type=None, from_folder: str = None):
	"""

	:param from_folder:
	:param start_date:
	:param end_date:
	:param data_type:
	:return:
	"""
	pass
