-- workflow=customeraddress
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=temp
-- target=variables
select from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'yyyy')                                as `YEAR`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'MM')                                  as `MONTH`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'dd')                                  as `DAY`;


-- step=2
-- source=mysql
--  dbName=bigdata_etl
--  tableName=CustomerAddress
-- target=hive
--  dbName=ods
--  tableName=CustomerAddress
-- writeMode=overwrite
-- skipFollowStepWhenEmpty=true
select CustomerID	AS CustomerID,
       AddressID	AS AddressID,
       AddressType	AS AddressType,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate,
       '${YEAR}' AS "year",
       '${MONTH}' AS "month",
       '${DAY}' AS "day"
from raw.CustomerAddress
where ModifiedDate >= '${DATA_RANGE_START}' and ModifiedDate < '${DATA_RANGE_END}'


-- step=3
-- source=hive
--  dbName=ods
--  tableName=CustomerAddress
-- target=hive
--  dbName=dw
--  tableName=CustomerAddress
-- writeMode=overwrite
select CustomerID	AS CustomerID,
       AddressID	AS AddressID,
       AddressType	AS AddressType,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate
       from ods.CustomerAddress
where `year` =  '${YEAR}' and `month` = '${MONTH}' and `day` = '${DAY}'
union all
select a.CustomerID	AS CustomerID,
       a.AddressID	AS AddressID,
       a.AddressType	AS AddressType,
       a.rowguid	AS rowguid,
       a.ModifiedDate	AS ModifiedDate
       from dw.CustomerAddress as a
left join ods.CustomerAddress as b on
       a.CustomerID = b.CustomerID and
       a.AddressID = b.AddressID 
       and b.`year` =  '${YEAR}' and b.`month` = '${MONTH}' and b.`day` = '${DAY}'
where b.CustomerID  is null
