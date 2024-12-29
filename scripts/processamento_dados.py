import json
import csv

class Dados:

    def __init__(self, path, type):
        self.path = path
        self.type = type
        self.dados = self.read_data()
        self.nome_colunas = self.get_columns()

    def __read_json(self):
        with open(self.path, 'r') as file:
            dados_json = json.load(file)
            return dados_json
    
    def __read_csv(self):
        dados_csv = []
        with open(self.path, 'r') as file:
            spamreader = csv.DictReader(file, delimiter = ',')
            for row in spamreader:
                dados_csv.append(row)
        return dados_csv

    def read_data(self):
        file_type = self.type.strip().lower()
        if file_type == 'csv':
            dados =  self.__read_csv()
        elif file_type == 'json':
            dados = self.__read_json()
        elif file_type == 'list':
            dados = self.path
            self.path = 'lista em memória'
        return dados
            
    def get_columns(self):
        colunas = list(self.dados[-1].keys())
        if 'Data da Venda' not in colunas:
            colunas.append('Data da Venda')
        return colunas
    
    def rename_columns(self, key_mapping):

        dados_tratados = []
        for old_dict in self.dados:
            new_dict = {}
            for old_key, values in old_dict.items():
                new_dict[key_mapping[old_key]] = values
            dados_tratados.append(new_dict)

        self.dados = dados_tratados
        self.nome_colunas = self.get_columns()

    def join_data(*args):
        dados_combinados = []
        for i in args:
            dados_combinados.extend(i.dados)
        return Dados(path = dados_combinados, type = 'list')
    
    def __transforming_table_data(self):
        nomes_colunas = list(self.dados[0].keys())
        data_transforming = []

        if 'Data da Venda' not in nomes_colunas:
            nomes_colunas.append('Data da Venda')

        for row in self.dados:
            linha = []
            for coluna in nomes_colunas:
                linha.append(row.get(coluna, 'Indisponivel'))
            data_transforming.append(linha)
        return data_transforming
    
    def create_file(self, path_to_save):
        with open(path_to_save, 'w') as file:
            writer = csv.writer(file)
            writer.writerow(self.get_columns())
            writer.writerows(self.__transforming_table_data())
 