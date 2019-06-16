from enum import Enum, auto


class FileType(Enum):
	TRECHOS_ITINERARIOS = auto()
	TABELA_VEICULO = auto()
	TABELA_LINHA = auto()
	SHAPE_LINHA = auto()
	PONTOS_LINHA = auto()
	POIS = auto()
	LINHAS = auto()
	VEICULOS = auto()
	ALL = auto()
