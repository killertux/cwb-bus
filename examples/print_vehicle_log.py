import asyncio
from cwbbus.downloader import *
from cwbbus.datareader import DataReader
from cwbbus.filetype import FileType

async def main():
	data = await get_data(date.fromisoformat("2019-06-29"), FileType.VEICULOS)
	reader = DataReader()
	reader.feed_data(data[FileType.VEICULOS], FileType.VEICULOS)
	print(reader.vehicle_log.describe())


if __name__ == "__main__":
	asyncio.run(main())
