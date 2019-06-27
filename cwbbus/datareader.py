import pandas as pd

_lines = {
	'bus_lines': {
		'map': {
			'COD': 'id',
			'NOME': 'name',
			'NOME_COR': 'color',
			'SOMENTE_CARTAO': 'card_only',
			"CATEGORIA_SERVICO": 'category'
		}
	}
}
_lines['bus_lines']['orig_cols'] = [*_lines['bus_lines']['map']]
_lines['bus_lines']['cols'] = [*_lines['bus_lines']['map'].values()]
_lines['bus_lines']['index'] = _lines['bus_lines']['map']['COD']

_line_stops = {
	'bus_stops': {
		'map': {
			'NUM': 'number',
			'NOME': 'name',
			'TIPO': 'type',
			'LAT': 'latitude',
			'LON': 'longitude'
		}
	},
	'itineraries': {
		'map': {
			'ITINERARY_ID': 'id',
			'COD': 'bus_line_id',
			'SENTIDO': 'direction'
		}
	}
}
_line_stops['bus_stops']['orig_cols'] = [*_line_stops['bus_stops']['map']]
_line_stops['bus_stops']['cols'] = [*_line_stops['bus_stops']['map'].values()]
_line_stops['bus_stops']['index'] = _line_stops['bus_stops']['map']['NUM']
_line_stops['itineraries']['orig_cols'] = [*_line_stops['itineraries']['map']]
_line_stops['itineraries']['cols'] = [*_line_stops['itineraries']['map'].values()]
_line_stops['itineraries']['index'] = _line_stops['itineraries']['map']['ITINERARY_ID']

_pois = {
	'points_of_interest': {
		'map': {
			'POI_NAME': 'name',
			'POI_DESC': 'description',
			'POI_CATEGORY_NAME': 'category',
			'POI_LAT': 'latitude',
			'POI_LON': 'longitude'
		}
	}
}
_pois['points_of_interest']['orig_cols'] = [*_pois['points_of_interest']['map']]
_pois['points_of_interest']['cols'] = [*_pois['points_of_interest']['map'].values()]
_pois['points_of_interest']['index'] = _pois['points_of_interest']['map']['POI_NAME']


class DataReader(object):
	def __init__(self):
		"""
		Stores all dataframes and provides methods to feed data into the dataframes.
		"""
		self.bus_lines = pd.DataFrame(columns=_lines['bus_lines']['cols'])

		# FIXME: colunas fixas
		self.bus_line_shapes = pd.DataFrame(columns=['id', 'bus_line_id', 'latitude', 'longitude'])

		self.bus_stops = pd.DataFrame(columns=_line_stops['bus_stops']['cols'])

		self.itineraries = pd.DataFrame(columns=_line_stops['itineraries']['cols'])

		# FIXME: colunas fixas
		self.itinerary_stops = pd.DataFrame(columns=['itinerary_id', 'sequence_number', 'stop_number'])

		self.schedule_tables = pd.DataFrame(columns=['table_id', 'bus_line_id', 'bus_stop_id', 'day_type', 'time',
		                                             'adaptive'])

		self.points_of_interest = pd.DataFrame(columns=_pois['points_of_interest']['cols'])

	def feed_linhas_json(self, filename: str):
		"""
		Reads a *_linhas.json file and merges the data into the bus_lines dataframe.
		:param filename: path to the file
		"""
		file_data = pd.read_json(filename)
		bus_line_data = file_data[_lines['bus_lines']['orig_cols']].copy()

		bus_line_data.rename(columns=_lines['bus_lines']['map'], inplace=True)

		self.bus_lines = self.bus_lines.merge(bus_line_data, how='outer')

	def feed_pois_json(self, filename):
		"""
		Reads a *_pois.json file and merges the data into the points_of_interest dataframe.
		:param filename: path to the file
		"""
		file_data = pd.read_json(filename)
		poi_data = file_data[_pois['points_of_interest']['orig_cols']].copy()

		poi_data.rename(columns=_pois['points_of_interest']['map'], inplace=True)

		self.points_of_interest = self.points_of_interest.merge(poi_data, how='outer')

	def feed_pontos_linha_json(self, filename):
		"""
		Reads a *_pontosLinha.json file and merges the data into the bus_stops dataframe.
		:param filename: path to the file
		"""
		file_data = pd.read_json(filename)
		bus_stop_data = file_data[_line_stops['bus_stops']['orig_cols']].copy()
		itinerary_data = file_data[_line_stops['itineraries']['orig_cols']].copy()
		itinerary_stops_data = file_data[['ITINERARY_ID', 'SEQ', 'NUM']].copy()

		bus_stop_data.rename(columns=_line_stops['bus_stops']['map'], inplace=True)
		bus_stop_data.drop_duplicates(inplace=True)

		itinerary_data.rename(columns=_line_stops['itineraries']['map'], inplace=True)
		itinerary_data.drop_duplicates(inplace=True)

		itinerary_stops_data.rename(columns={
			'ITINERARY_ID': 'itinerary_id',
			'SEQ': 'sequence_number',
			'NUM': 'stop_number'
		}, inplace=True)
		itinerary_stops_data.drop_duplicates(inplace=True)

		self.bus_stops = self.bus_stops.merge(bus_stop_data, how='outer')
		self.itineraries = self.itineraries.merge(itinerary_data, how='outer')
		self.itinerary_stops = self.itinerary_stops.merge(itinerary_stops_data, how='outer')

	def feed_shape_linha_json(self, filename):
		file_data = pd.read_json(filename)
		bus_line_shape_data = file_data[['SHP', 'COD', 'LAT', 'LON']].copy()

		bus_line_shape_data.rename(columns={
			'SHP': 'id',
			'COD': 'bus_line_id',
			'LAT': 'latitude',
			'LON': 'longitude'
		}, inplace=True)

		self.bus_line_shapes = bus_line_shape_data

	def feed_tabela_linha_json(self, filename):
		file_data = pd.read_json(filename)
		schedule_table_data = file_data[['TABELA', 'COD', 'NUM', 'DIA', 'HORA', 'ADAPT']].copy()

		schedule_table_data.rename(columns={
			'TABELA': 'table_id',
			'COD': 'bus_line_id',
			'NUM': 'bus_stop_id',
			'DIA': 'day_type',
			'HORA': 'time',
			'ADAPT': 'adaptive'
		}, inplace=True)
		schedule_table_data.replace({'day_type': {
			1: 'weekday',
			2: 'saturday',
			3: 'sunday',
			4: 'holiday'
		}}, inplace=True)

		self.schedule_tables = self.schedule_tables.merge(schedule_table_data, how='outer')

	def feed_tabela_veiculo_json(self, filename):
		pass

	def feed_trechos_itinerarios_json(self, filename):
		pass

	def feed_veiculos_json(self, filename):
		pass
