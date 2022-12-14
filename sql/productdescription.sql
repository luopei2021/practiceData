-- workflow=productdescription
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
--  tableName=ProductDescription
-- target=hive
--  dbName=ods
--  tableName=ProductDescription
-- writeMode=overwrite
-- skipFollowStepWhenEmpty=true
select ProductDescriptionID	AS ProductDescriptionID,
       Description	AS Description,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate,
       '${YEAR}' AS "year",
       '${MONTH}' AS "month",
       '${DAY}' AS "day"
from raw.ProductDescription
where ModifiedDate >= '${DATA_RANGE_START}' and ModifiedDate < '${DATA_RANGE_END}'


-- step=3
-- source=hive
--  dbName=ods
--  tableName=ProductDescription
-- target=hive
--  dbName=dw
--  tableName=ProductDescription
-- writeMode=overwrite
select ProductDescriptionID	AS ProductDescriptionID,
       Description	AS Description,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate
       from ods.ProductDescription
where `year` =  '${YEAR}' and `month` = '${MONTH}' and `day` = '${DAY}'
union all
select a.ProductDescriptionID	AS ProductDescriptionID,
       a.Description	AS Description,
       a.rowguid	AS rowguid,
       a.ModifiedDate	AS ModifiedDate
       from dw.ProductDescription as a
left join ods.ProductDescription as b on
       a.ProductDescriptionID = b.ProductDescriptionID 
       and b.`year` =  '${YEAR}' and b.`month` = '${MONTH}' and b.`day` = '${DAY}'
where b.ProductDescriptionID  is null
