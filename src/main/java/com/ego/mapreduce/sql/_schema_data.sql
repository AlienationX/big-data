create table tmp.mr_detail stored as parquet as
select 1 as id,	1 as order_id, '2020-01-01 12:36:01.0' as charge_time, '1' as item_id, 'milk'   as item_name, 5    as amount union all
select 2 as id,	2 as order_id, '2020-01-02 11:55:51.0' as charge_time, '1' as item_id, 'milk'   as item_name, 5    as amount union all
select 3 as id,	2 as order_id, '2020-01-02 11:55:51.0' as charge_time, '2' as item_id, 'bread'  as item_name, 10   as amount union all
select 4 as id,	2 as order_id, '2020-01-02 11:55:51.0' as charge_time, '3' as item_id, 'orange' as item_name, 3    as amount union all
select 5 as id,	3 as order_id, '2020-01-03 12:13:00.0' as charge_time, '2' as item_id, 'bread'  as item_name, 12.5 as amount union all
select 6 as id,	4 as order_id, '2020-01-01 09:39:48.0' as charge_time, '3' as item_id, 'orange' as item_name, 6    as amount union all
select 7 as id,	4 as order_id, '2020-01-01 09:39:48.0' as charge_time, '3' as item_id, 'orange' as item_name, 6    as amount union all
select 8 as id,	4 as order_id, '2020-01-06 09:39:48.0' as charge_time, '3' as item_id, 'orange' as item_name, NULL as amount;

create table tmp.mr_order stored as parquet as
select 1 as id, '2020-01-01 12:33:01.0' as order_time, 'bill' as client_name, '01' as status union all
select 2 as id,	'2020-01-02 11:55:38.0' as order_time, 'bill' as client_name, '01' as status union all
select 3 as id,	'2020-01-03 12:12:42.0' as order_time, 'bill' as client_name, '01' as status union all
select 4 as id,	'2020-01-01 09:39:12.0' as order_time, 'hank' as client_name, '01' as status union all
select 5 as id,	'2020-01-02 08:58:53.0' as order_time, 'hank' as client_name, '02' as status;

create table tmp.mr_user stored as parquet as
select 1 as user_id, 'bill' as user_name, 'male'   as gender, '1999-12-31' as birthday union all
select 2 as user_id, 'alis' as user_name, 'female' as gender, '1963-08-02' as birthday union all
select 3 as user_id, 'hank' as user_name, 'male'   as gender, '1991-01-13' as birthday;
