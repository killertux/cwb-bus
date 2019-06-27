import asyncio
import async_timeout
import aiohttp
from lzma import decompress
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
	Will try to grab from the `base_url` plus the types specified and append the .xz extension

	:param data_date: A :class:`datetime.date` instance with the specific date for the data that wants to be gathered
	:param session: A :class:`aiohttp.ClientSession` session for downloading. Should probably only instance 1 for an entire operation
	:param data_type: What type of data to download. See :class:`FileType` for available types
	:param base_url: The base url for downloading data. Shouldn't need to be changed ever
	:param timeout: Timeout value for the download until giving up
	:return: a compressed `bytes` object
	"""
	with async_timeout.timeout(timeout):
		async with session.get(f'{base_url}{data_date.strftime("%Y_%m_%d")}_{data_type.value}.xz') as response:
			file = b''
			while True:
				chunk = await response.content.read(1024)
				if not chunk:
					break
				file += chunk

			return await response.release()


async def get_data(data_date: date, data_type: FileType = None, from_folder: str = None):
	"""

	:param from_folder:
	:param data_date:
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
