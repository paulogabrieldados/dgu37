-- models/bronze/bandcamp_bronze.sql
{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('bandcamp_sales_project', 'bandcamp_sales') }}
)

select *
from source
