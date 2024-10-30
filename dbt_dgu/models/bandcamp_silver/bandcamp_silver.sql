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

                          