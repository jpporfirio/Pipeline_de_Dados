from processamento_dados import Dados

# Para a construção do objeto apenas insira o caminho do arquivo e o tipo

path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'

dados_empresa_A = Dados(path = path_json, type = 'json')
print(f'Amostra dos dados json: \n{dados_empresa_A.dados[0]} \n\n')

dados_empresa_B = Dados(path = path_csv, type = 'csv')
print(f'Amostra dos dados csv: \n{dados_empresa_A.dados[0]} \n\n')

# Dicionário para realizer a normalização dos título das colunas 
key_mapping = {'Nome do Item': 'Nome do Produto',
                    'Classificação do Produto': 'Categoria do Produto',
                    'Valor em Reais (R$)': 'Preço do Produto (R$)',
                    'Quantidade em Estoque': 'Quantidade em Estoque',
                    'Nome da Loja': 'Filial',
                    'Data da Venda': 'Data da Venda'}

# Método resposável pela normalização do título das colunas
print(f'Note o nome das chaves antes da utilização do metodo: \n{dados_empresa_B.dados[0]} \n')
dados_empresa_B.rename_columns(key_mapping = key_mapping)
print(f'Note o nome das chaves depois da utilização do metodo: \n{dados_empresa_B.dados[0]} \n\n')

# Metodo para a junção dos arquivos
dados_fusao = Dados.join_data(dados_empresa_A, dados_empresa_B)
print('Contagem da quantidade dos dados antes e depois do uso do metodo')
print(f'Quantidade de dados 1º empresa: {len(dados_empresa_A.dados)}')
print(f'Quantidade de dados 2º empresa: {len(dados_empresa_B.dados)}')
print(f'Quantidade com os dados unidos: {len(dados_fusao.dados)}')

# Criação dos arquivos em CSV (Apenas insira o caminho que desenha criar o arquivo)
dados_fusao.create_file('data_processed/dados_tratados')
