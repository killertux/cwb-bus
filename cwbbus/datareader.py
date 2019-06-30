import pandas as pd

# TODO: Ver o que pode ser otimizado. Nos dados que vêm de um único arquivo, pode-se utilizar concat em vez de merge.

class DataReader(object):
	def __init__(self):
		"""
		Stores all dataframes and provides methods to feed data into the dataframes.
		"""
		self.bus_lines = pd.DataFrame(columns=['id', 'name', 'color', 'card_only', 'category'])
		self.bus_line_shapes = pd.DataFrame(columns=['id', 'bus_line_id', 'latitude', 'longitude'])
		self.bus_stops = pd.DataFrame(columns=['number', 'name', 'type', 'latitude', 'longitude'])
		self.itineraries = pd.DataFrame(columns=['id', 'bus_line_id', 'direction'])
		self.itinerary_stops = pd.DataFrame(columns=['itinerary_id', 'sequence_number', 'stop_number'])
		self.bus_lines_schedule_tables = pd.DataFrame(columns=['table_id', 'bus_line_id', 'bus_stop_id', 'day_type',
		                                                       'time', 'adaptive'])
		self.vehicles_schedule_tables = pd.DataFrame(columns=['table_id', 'bus_line_id', 'bus_stop_id', 'vehicle_id',
		                                                      'time'])
		self.itinerary_stops_extra = pd.DataFrame(columns=['itinerary_id', 'itinerary_name', 'bus_line_id',
		                                                   'itinerary_stop_id', 'stop_name', 'stop_name_short',
		                                                   'stop_name_abbr', 'bus_stop_id', 'sequence_number', 'type',
		                                                   'special_stop'])
		self.itinerary_distances = pd.DataFrame(columns=['itinerary_stop_id', 'itinerary_next_stop_id', 'distance_m'])
		self.companies = pd.DataFrame(columns=['id', 'name'])
		self.itinerary_stops_companies = pd.DataFrame(columns=['itinerary_stop_id', 'company_id'])
		self.vehicle_log = pd.DataFrame(columns=['timestamp', 'vehicle_id', 'bus_line_id', 'latitude', 'longitude'])
		self.points_of_interest = pd.DataFrame(columns=['name', 'description', 'category', 'latitude', 'longitude'])

	def feed_linhas_json(self, filename: str):
		"""
		Reads a *_linhas.json file and merges the data into the bus_lines dataframe.
		:param filename: path to the file
		"""
		file_data = pd.read_json(filename)
		bus_line_data = file_data[['COD', 'NOME', 'NOME_COR', 'SOMENTE_CARTAO', 'CATEGORIA_SERVICO']].copy()

		bus_line_data.rename(columns={
			'COD': 'id',
			'NOME': 'name',
			'NOME_COR': 'color',
			'SOMENTE_CARTAO': 'card_only',
			"CATEGORIA_SERVICO": 'category'
		}, inplace=True)

		self.bus_lines = self.bus_lines.merge(bus_line_data, how='outer')

	def feed_pois_json(self, filename):
		"""
		Reads a *_pois.json file and merges the data into the points_of_interest dataframe.
		:param filename: path to the file
		"""
		file_data = pd.read_json(filename)
		poi_data = file_data[['POI_NAME', 'POI_DESC', 'POI_CATEGORY_NAME', 'POI_LAT', 'POI_LON']].copy()

		poi_data.rename(columns={
			'POI_NAME': 'name',
			'POI_DESC': 'description',
			'POI_CATEGORY_NAME': 'category',
			'POI_LAT': 'latitude',
			'POI_LON': 'longitude'
		}, inplace=True)

		self.points_of_interest = self.points_of_interest.merge(poi_data, how='outer')

	def feed_pontos_linha_json(self, filename):
		"""
		Reads a *_pontosLinha.json file and merges the data into the bus_stops dataframe.
		:param filename: path to the file
		"""
		file_data = pd.read_json(filename)
		bus_stop_data = file_data[['NUM', 'NOME', 'TIPO', 'LAT', 'LON']].copy()
		itinerary_data = file_data[['ITINERARY_ID', 'COD', 'SENTIDO']].copy()
		itinerary_stops_data = file_data[['ITINERARY_ID', 'SEQ', 'NUM']].copy()

		bus_stop_data.rename(columns={
			'NUM': 'number',
			'NOME': 'name',
			'TIPO': 'type',
			'LAT': 'latitude',
			'LON': 'longitude'
		}, inplace=True)
		bus_stop_data.drop_duplicates(inplace=True)

		itinerary_data.rename(columns={
			'ITINERARY_ID': 'id',
			'COD': 'bus_line_id',
			'SENTIDO': 'direction'
		}, inplace=True)
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
		# TODO: Add file date to the data?

		self.bus_lines_schedule_tables = self.bus_lines_schedule_tables.merge(schedule_table_data, how='outer')

	def feed_tabela_veiculo_json(self, filename):
		file_data = pd.read_json(filename)
		schedule_table_data = file_data[['TABELA', 'COD_LINHA', 'COD_PONTO', 'HORARIO', 'VEICULO']].copy()

		schedule_table_data.rename(columns={
			'TABELA': 'table_id',
			'COD_LINHA': 'bus_line_id',
			'COD_PONTO': 'bus_stop_id',
			'HORARIO': 'time',
			'VEICULO': 'vehicle_id'
		}, inplace=True)
		schedule_table_data['bus_line_id'] = schedule_table_data['bus_line_id'].astype(str)
		# TODO: Add file date to the data?

		self.vehicles_schedule_tables = self.vehicles_schedule_tables.merge(schedule_table_data, how='outer')

	def feed_trechos_itinerarios_json(self, filename):
		file_data = pd.read_json(filename)
		itinerary_stops_data = file_data[['COD_ITINERARIO', 'NOME_ITINERARIO', 'COD_LINHA', 'CODIGO_URBS', 'STOP_NAME',
		                                  'NOME_PTO_PARADA_TH', 'NOME_PTO_ABREVIADO', 'STOP_CODE', 'SEQ_PONTO_TRECHO_A',
		                                  'TIPO_TRECHO', 'PTO_ESPECIAL']].copy()
		itinerary_distances_data = file_data[['CODIGO_URBS', 'COD_PTO_TRECHO_B', 'EXTENSAO_TRECHO_A_ATE_B']].copy()
		company_data = file_data[['COD_EMPRESA', 'NOME_EMPRESA']].copy()
		itinerary_stops_company_data = file_data[['CODIGO_URBS', 'COD_EMPRESA']].copy()

		itinerary_stops_data.rename(columns={
			'COD_ITINERARIO': 'itinerary_id',
			'NOME_ITINERARIO': 'itinerary_name',
			'COD_LINHA': 'bus_line_id',
			'CODIGO_URBS': 'itinerary_stop_id',
			'STOP_NAME': 'stop_name',
			'NOME_PTO_PARADA_TH': 'stop_name_short',
			'NOME_PTO_ABREVIADO': 'stop_name_abbr',
			'STOP_CODE': 'bus_stop_id',
			'SEQ_PONTO_TRECHO_A': 'sequence_number',
			'TIPO_TRECHO': 'type',
			'PTO_ESPECIAL': 'special_stop'
		}, inplace=True)
		itinerary_stops_data.drop_duplicates(inplace=True)

		itinerary_distances_data.rename(columns={
			'CODIGO_URBS': 'itinerary_stop_id',
			'COD_PTO_TRECHO_B': 'itinerary_next_stop_id',
			'EXTENSAO_TRECHO_A_ATE_B': 'distance_m'
		}, inplace=True)
		itinerary_distances_data.drop_duplicates(inplace=True)

		company_data.rename(columns={
			'COD_EMPRESA': 'id',
			'NOME_EMPRESA': 'name'
		}, inplace=True)
		company_data.drop_duplicates(inplace=True)

		itinerary_stops_company_data.rename(columns={
			'CODIGO_URBS': 'itinerary_stop_id',
			'COD_EMPRESA': 'company_id'
		}, inplace=True)
		itinerary_stops_company_data.drop_duplicates(inplace=True)

		self.itinerary_stops_extra = self.itinerary_stops_extra.merge(itinerary_stops_data, how='outer')
		self.itinerary_distances = self.itinerary_distances.merge(itinerary_distances_data, how='outer')
		self.companies = self.companies.merge(company_data, how='outer')
		self.itinerary_stops_companies = self.itinerary_stops_companies.merge(itinerary_stops_company_data, how='outer')

	def feed_veiculos_json(self, filename):
		vehicle_log_data = pd.read_json(filename, lines=True)
		vehicle_log_data.rename(columns={
			'DTHR': 'timestamp',
			'VEIC': 'vehicle_id',
			'COD_LINHA': 'bus_line_id',
			'LAT': 'latitude',
			'LON': 'longitude'
		}, inplace=True)

		vehicle_log_data['timestamp'] = pd.to_datetime(vehicle_log_data['timestamp'], format='%d/%m/%Y %H:%M:%S')

		# FIXME: these datasets are too large. How to deal with concatenation?
		# self.vehicle_log = pd.concat([self.vehicle_log, vehicle_log_data], sort=False)
		self.vehicle_log = vehicle_log_data
