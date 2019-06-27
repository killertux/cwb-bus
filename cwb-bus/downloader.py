import asyncio
import aiohttp
from lzma import decompress as decompress_xz
from zlib import decompress as decompress_gzip
from datetime import date
from .filetype import FileType


#_session = aiohttp.ClientSession()
_download_folder = "./cache/downloads/"


async def _download_file(data_date: date, data_type: FileType, session: aiohttp.ClientSession,
                         base_url: str = "http://dadosabertos.c3sl.ufpr.br/curitibaurbs/",
                         timeout: int = 10) -> bytes:
	"""
	Internal function that downloads the data.

	This function will try to grab the data from the server using asynchronous requests.
	Will try to grab from the `base_url` plus the types specified and append an extension.
	It will first try the `.xz` extension as it is more common. However, it will also try the `.tar.gz`
	extension as it was also used early on.

	:param data_date: A :class:`datetime.date` instance with the specific date for the data that wants to be gathered
	:param session: A :class:`aiohttp.ClientSession` session for downloading. Should probably only instance 1 for an entire operation
	:param data_type: What type of data to download. See :class:`FileType` for available types
	:param base_url: The base url for downloading data. Shouldn't need to be changed ever
	:param timeout: Timeout value for the download until giving up
	:return: a `bytes` compressed object, which needs to be decompressed. See :function:`_uncompress_file`
	"""
	pass


def _decompress_file(file: bytes, is_xz: bool = True) -> bytes:
	"""
	Simple internal function as a small wrapper for decompressing files

	:param file: The bytes like object for the file received from the downloader
	:param is_xz: Whether the file is a xz type file (if True) or a tar.gz file (if False)
	:return: The compressed bytes object
	"""
	if is_xz:
		return decompress_xz(file)
	# TODO Treat tar.gz files because they are going to return a tar file
	# else:
	# 	yield decompress_gzip(file)


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
