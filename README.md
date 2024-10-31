# Projeto Bandcamp's Dataset - DGU 37

## *Projeto de ETL + Pipeline de Dados + Visualização de Dados*

## *Visão Geral*
 - Este projeto tem como objetivo analisar as vendas de música na plataforma Bandcamp.
 - Processo de ETL no Python + dbt
 - Utilizei o dbt para criar camadas de dados (bronze, silver, e gold)
 - Visualizei os resultados com o Briefer.

 ## *Estrutura do Projeto*

### *Tratamanto dos dados no Python* 

``` python
import pandas as pd

# Caminho do arquivo original
csv_file_path = 'C:/Users/PauloGomes/Desktop/ProjetoDGU/1000000-bandcamp-sales.csv'
# Caminho para salvar o novo arquivo
cleaned_csv_file_path = 'C:/Users/PauloGomes/Desktop/ProjetoDGU/bandcamp_sales_tratado.csv'

# Ler o arquivo original
df = pd.read_csv(csv_file_path)

# Remover todos os caracteres que não são dígitos ou ponto nas colunas de valor monetário
df['amount_paid_fmt'] = df['amount_paid_fmt'].replace(r'[^\d.]', '', regex=True).astype(float)
df['item_price'] = df['item_price'].replace(r'[^\d.]', '', regex=True).astype(float)
df['amount_paid'] = df['amount_paid'].replace(r'[^\d.]', '', regex=True).astype(float)
df['amount_paid_usd'] = df['amount_paid_usd'].replace(r'[^\d.]', '', regex=True).astype(float)
df['amount_over_fmt'] = df['amount_over_fmt'].replace(r'[^\d.]', '', regex=True).astype(float)

# Converter Unix timestamp para formato datetime
df['utc_date'] = pd.to_datetime(df['utc_date'], unit='s', errors='coerce')
df['date'] = df['utc_date'].dt.strftime('%Y-%m-%d')
df['hora'] = df["utc_date"].dt.strftime('%H:%M')

item_type = {"a":"albuns",
             "p": "physical item",
             "t": "tracks"}

slug_type = {"a":"albums",
             "p": "merch",
             "t": "tracks"}

df['item_type'] = df['item_type'].map(item_type).fillna('unknown')
df["slug_type"] = df['slug_type'].map(slug_type).fillna('unknown')


#Retirando as colunas unamed:0 e utc_date
df = df.drop(['utc_date',
              'Unnamed: 0', 
              'art_url', 
              'track_album_slug_text',
              'addl_count',
              'amount_paid_fmt', 
              'amount_paid', 
              'art_id', 
              'url', 
              'package_image_id', 
              'item_slug',
              'releases',
              'amount_over_fmt',
              'album_title'           
              ], 
              axis= 1)


# Salvar o CSV processado
df.to_csv(cleaned_csv_file_path, index=False)

print(f"CSV processado e salvo em {cleaned_csv_file_path}")

```

## *Ingestão dos dados*

- Realizei o carregamento dos dados para o banco com Python utilizando o SQL Alchemy.

``` Python
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

# String de conexão
conexao_str = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}?client_encoding=utf8'

# Engine
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

```

## *Após o carregamento do dados fiz as camadas de dados para consumir no Briefer*
### *Camada Bronze: Dados brutos importados.*
```sql
{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('bandcamp_sales_project', 'bandcamp_sales') }}
)

select *
from source

```

### *Camada Silver: Transformação e limpeza dos dados.*
```sql
-- models/silver/bandcamp_silver.sql
{{ config(materialized='view') }}

with bronze as (
    select *
    from {{ ref('bandcamp_bronze') }}
)

select
    _id as id,  -- Texto, mantendo como VARCHAR
    item_type,
    country_code,
    country,
    slug_type,
    cast(item_price as NUMERIC(10, 2)) as item_price,  -- Formatado como NUMERIC
    item_description,
    artist_name,
    currency,
    cast(amount_paid_usd as NUMERIC(10, 2)) as amount_paid_usd,  -- Formatado como NUMERIC
    cast(date as DATE) as date,  -- Formatado como DATE
    cast(hora as TIME) as hora   -- Formatado como TIME
from {{ ref('bandcamp_bronze') }}

```

### *Camada Gold: Agregação e análise final dos dados.*

```sql
-- models/gold/bandcamp_gold.sql
{{ config(materialized='view') }}

with silver_data as (
    -- Seleciona todos os dados da camada Silver
    select * 
    from {{ ref('bandcamp_silver') }}
),

-- Total de vendas por artista
artist_sales as (
    select
        artist_name,
        sum(amount_paid_usd) as total_sales_usd
    from silver_data
    group by artist_name
),

-- Vendas por país
country_sales as (
    select
        country,
        sum(amount_paid_usd) as total_sales_usd,
        count(*) as total_transactions
    from silver_data
    group by country
),

-- Artistas mais vendidos por país
top_artists_by_country as (
    select
        country,
        artist_name,
        sum(amount_paid_usd) as total_sales_usd
    from silver_data
    group by country, artist_name
),

-- Vendas por tipo de item
item_sales as (
    select
        item_type,
        sum(amount_paid_usd) as total_sales_usd,
        count(*) as total_transactions
    from silver_data
    group by item_type
),

-- Vendas diárias
daily_sales as (
    select
        date,
        sum(amount_paid_usd) as total_sales_usd,
        count(*) as total_transactions
    from silver_data
    group by date
),

-- Média de valor pago por transação
average_transaction as (
    select
        avg(amount_paid_usd) as avg_amount_per_transaction,
        count(*) as total_transactions
    from silver_data
)

-- Seleciona os dados agregados finais
select
    a.artist_name,
    a.total_sales_usd as total_sales_artist,
    c.country,
    c.total_sales_usd as total_sales_country_usd,
    c.total_transactions as total_transactions_country,
    t.artist_name as top_artist_by_country,
    t.total_sales_usd as top_artist_sales_country_usd,
    i.item_type,
    i.total_sales_usd as total_sales_item_usd,
    d.date,
    d.total_sales_usd as daily_sales_usd,
    avg.avg_amount_per_transaction,
    avg.total_transactions
from artist_sales a
left join country_sales c on true -- Realiza o JOIN com country_sales sem uma relação direta
left join top_artists_by_country t on c.country = t.country
left join item_sales i on true -- Realiza o JOIN com item_sales sem uma relação direta
left join daily_sales d on true -- Realiza o JOIN com daily_sales sem uma relação direta
left join average_transaction avg on true

```

## *Visualização dos Dados:*

- A visulização dos dados foi feita através do Briefer.
- Utilizando os notebooks com SQL para fazer a consulta dos dados e usando a ferramenta de visualização que está atrelada ao notebook.

- Obs: A visualização foi feita em cima da camada Silver, pois ao realizar as consultas na camada gold , o banco acusava baixa memória por ser tratar de uma versão free do Render.

![Bandcamp Dashboard 1](https://github.com/paulogabrieldados/dgu37/blob/main/bandcamp_dash_pg1.png)

![Bandcamp Dashboard 2](https://github.com/paulogabrieldados/dgu37/blob/main/bandcamp_dash_pg2.png)


## *Ferramentas Utilizadas*

- Python: Para Extração do csv, Transformação e Upload.

- dbt: Para a modelagem e transformação dos dados.

- PostgreSQL: Banco de dados para armazenar os dados.

- Briefer: Para criação de dashboards interativos e colaboração.

## Próximos Passos:

- Otimizar as querys.
- Otimizar a utilização do armazenamento do banco de dados.





