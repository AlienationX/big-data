create database if not exists tmp;
use tmp;

create table if not exists tmp.mr_order (
    id              bigint,
    order_time      timestamp,
    client_name     string,
    status          string
)
comment '订单表'
stored as parquet;

create table if not exists tmp.mr_detail (
    id              bigint,
    order_id        bigint,
    charge_time     timestamp,
    item_id         int,
    item_name       string,
    amount          double
)
comment '订单明细表'
stored as parquet;

insert overwrite table tmp.mr_order
select 1 as id,'2020-01-01 12:33:01' as order_time, 'bill' as client_name, '01' as status union all
select 2 as id,'2020-01-02 11:55:38' as order_time, 'bill' as client_name, '01' as status union all
select 3 as id,'2020-01-03 12:12:42' as order_time, 'bill' as client_name, '01' as status union all
select 4 as id,'2020-01-01 09:39:12' as order_time, 'hank' as client_name, '01' as status union all
select 5 as id,'2020-01-02 08:58:53' as order_time, 'hank' as client_name, '02' as status;

insert overwrite table tmp.mr_detail
select 1 as id, 1 as order_id, '2020-01-01 12:36:01' as charge_time,1 as item_id, 'milk' as item,5 as amount union all
select 2 as id, 2 as order_id, '2020-01-02 11:55:51' as charge_time,1 as item_id, 'milk' as item,5 as amount union all
select 3 as id, 2 as order_id, '2020-01-02 11:55:51' as charge_time,2 as item_id, 'bread' as item,10 as amount union all
select 4 as id, 2 as order_id, '2020-01-02 11:55:51' as charge_time,3 as item_id, 'orange' as item,3 as amount union all
select 5 as id, 3 as order_id, '2020-01-03 12:13:00' as charge_time,2 as item_id, 'bread' as item,12.5 as amount union all
select 6 as id, 4 as order_id, '2020-01-01 09:39:48' as charge_time,3 as item_id, 'orange' as item,6 as amount union all
select 7 as id, 4 as order_id, '2020-01-01 09:39:48' as charge_time,3 as item_id, 'orange' as item,6 as amount union all
select 7 as id, 4 as order_id, null                  as charge_time,3 as item_id, 'orange' as item,null as amount;