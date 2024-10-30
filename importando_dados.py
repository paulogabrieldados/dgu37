import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Configurar banco de dados PostgreSQL a partir das variáveis de ambiente
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# Crie a string de conexão com a codificação UTF-8 explicitamente definida
conexao_str = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}?client_encoding=utf8'

# Crie a engine do SQLAlchemy
engine = create_engine(conexao_str)

try:
    # Testar a conexão ao banco de dados
    with engine.connect() as conn:
        print("Conexão estabelecida com sucesso!")

    # Caminho do arquivo CSV processado
    cleaned_csv_file_path = 'C:/Users/PauloGomes/Desktop/ProjetoDGU/bandcamp_sales_tratado.csv'
    # Carregar o CSV em um DataFrame do pandas
    df = pd.read_csv(cleaned_csv_file_path)

    # Importar o DataFrame para a tabela PostgreSQL
    df.to_sql('bandcamp_sales', engine, if_exists='replace', index=False)

    print("Dados importados com sucesso!")
except SQLAlchemyError as e:
    print(f"Erro ao conectar ou executar operações no banco de dados: {e}")

