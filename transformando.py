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