create database if not exists tmp;

use tmp;

create table tmp.mr_group stored as parquet as
select 'roger' as name,'2020-01-01' as dt,'milk' as item,5 as amount union all
select 'roger' as name,'2020-01-02' as dt,'milk' as item,5 as amount union all
select 'roger' as name,'2020-01-02' as dt,'bread' as item,10 as amount union all
select 'roger' as name,'2020-01-02' as dt,'orange' as item,3 as amount union all
select 'mike' as name,'2020-01-01' as dt,'bread' as item,12.5 as amount union all
select 'mike' as name,'2020-01-02' as dt,'orange' as item,6 as amount union all
select 'mike' as name,'2020-01-03' as dt,'orange' as item,6 as amount;