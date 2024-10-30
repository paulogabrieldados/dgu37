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
https://open.spotify.com/track/2y0EaLcd7TtUNomAdlLOLT
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


