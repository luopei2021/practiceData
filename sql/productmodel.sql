-- workflow=productmodel
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
--  tableName=ProductModel
-- target=hive
--  dbName=ods
--  tableName=ProductModel
-- writeMode=overwrite
-- skipFollowStepWhenEmpty=true
select ProductModelID	AS ProductModelID,
       Name	AS Name,
       CatalogDescription	AS CatalogDescription,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate,
       '${YEAR}' AS "year",
       '${MONTH}' AS "month",
       '${DAY}' AS "day"
from raw.ProductModel
where ModifiedDate >= '${DATA_RANGE_START}' and ModifiedDate < '${DATA_RANGE_END}'


-- step=3
-- source=hive
--  dbName=ods
--  tableName=ProductModel
-- target=hive
--  dbName=dw
--  tableName=ProductModel
-- writeMode=overwrite
select ProductModelID	AS ProductModelID,
       Name	AS Name,
       CatalogDescription	AS CatalogDescription,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate
       from ods.ProductModel
where `year` =  '${YEAR}' and `month` = '${MONTH}' and `day` = '${DAY}'
union all
select a.ProductModelID	AS ProductModelID,
       a.Name	AS Name,
       a.CatalogDescription	AS CatalogDescription,
       a.rowguid	AS rowguid,
       a.ModifiedDate	AS ModifiedDate
       from dw.ProductModel as a
left join ods.ProductModel as b on
       a.ProductModelID = b.ProductModelID 
       and b.`year` =  '${YEAR}' and b.`month` = '${MONTH}' and b.`day` = '${DAY}'
where b.ProductModelID  is null
