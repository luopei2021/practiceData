-- workflow=test
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- stepId=1
-- sourceConfig
--  dataSourceType=temp
-- targetConfig
--  dataSourceType=variables
-- checkPoint=false
-- dateRangeInterval=0
select from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'yyyy')                                as `YEAR`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'MM')                                  as `MONTH`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'dd')                                  as `DAY`;


-- step=2
-- source=mysql
--  dbName=raw
--  tableName=ProductDescription
-- target=hive
--  dbName=default
--  tableName=ProductDescription_his
-- writeMode=overwrite
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
--  dbName=default
--  tableName=ProductDescription_his
-- target=hive
--  dbName=default
--  tableName=ProductDescription_curr
-- writeMode=overwrite
select ProductDescriptionID	AS ProductDescriptionID,
       Description	AS Description,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate
       from ProductDescription_his
where `year` =  '${YEAR}' and `month` = '${MONTH}' and `day` = '${DAY}'
union all
select ProductDescriptionID	AS ProductDescriptionID,
       Description	AS Description,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate
       from ProductDescription_curr as a
left join ProductDescription_his as b on
       a.ProductID = b.ProductID 
       and b.`year` =  '${YEAR}' and b.`month` = '${MONTH}' and b.`day` = '${DAY}'
where b.ProductID  is null