-- workflow=report_3
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=hive
--  dbName=dw
--  tableName=SalesOrder
-- target=hive
--  dbName=dm
--  tableName=report3
-- writeMode=overwrite
with distinct_order as (select SalesOrderID
      ,max(ShipDate) as maxShipDate
      ,min(OrderDate) as minOrderDate
      ,datediff(max(ShipDate),min(OrderDate))  as sale_period
from raw.SalesOrder
group by SalesOrderID
),
rank_order as (
select SalesOrderID
,maxShipDate
,minOrderDate
,sale_period
,RANK() OVER (ORDER BY sale_period) as rank_number
from distinct_order)

select SalesOrderID
,maxShipDate
,minOrderDate
,sale_period
,rank_number
from rank_order
where rank_number<=10;