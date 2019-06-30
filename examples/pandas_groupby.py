import asyncio
from cwbbus.downloader import *
from cwbbus.datareader import DataReader
from cwbbus.filetype import FileType

async def main():
	reader = DataReader()
	async for day, data in get_data_range(date.fromisoformat("2019-05-01"), date.fromisoformat("2019-05-29"), FileType.LINHAS):
		reader.feed_data(data[FileType.LINHAS], FileType.LINHAS)
	print(reader.bus_lines.groupby(['color']).agg(['count']))


if __name__ == "__main__":
	asyncio.run(main())
