import asyncio
import aiohttp 
from datetime import datetime
from filetype import FileType


_session = aiohttp.ClientSession()
_download_folder = "./cache/downloads/"


async def _download_file(date: str, data_type: FileType = None,
                         base_url: str = "http://dadosabertos.c3sl.ufpr.br/curitibaurbs/"):
	"""

	:param date:
	:param data_type:
	:param base_url:
	:param download_folder:
	:return:
	"""
	pass


async def _uncompress_file(file):
	"""

	:param file:
	:param keep:
	:return:
	"""
	pass


async def get_data(date: str, data_type: FileType=None):
	"""

	:param date:
	:param data_type:
	:return:
	"""
	pass

async def get_data_range(start_date: str, end_date: str, data_type=None):
	"""

	:param start_date:
	:param end_date:
	:param data_type:
	:return:
	"""
	pass
