-- workflow=productmodelproductdescription
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
--  tableName=ProductModelProductDescription
-- target=hive
--  dbName=ods
--  tableName=ProductModelProductDescription
-- writeMode=overwrite
-- skipFollowStepWhenEmpty=true
select ProductModelID	AS ProductModelID,
       ProductDescriptionID	AS ProductDescriptionID,
       Culture	AS Culture,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate,
       '${YEAR}' AS "year",
       '${MONTH}' AS "month",
       '${DAY}' AS "day"
from raw.ProductModelProductDescription
where ModifiedDate >= '${DATA_RANGE_START}' and ModifiedDate < '${DATA_RANGE_END}'


-- step=3
-- source=hive
--  dbName=ods
--  tableName=ProductModelProductDescription
-- target=hive
--  dbName=dw
--  tableName=ProductModelProductDescription
-- writeMode=overwrite
select ProductModelID	AS ProductModelID,
       ProductDescriptionID	AS ProductDescriptionID,
       Culture	AS Culture,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate
       from ods.ProductModelProductDescription
where `year` =  '${YEAR}' and `month` = '${MONTH}' and `day` = '${DAY}'
union all
select ProductModelID	AS ProductModelID,
       ProductDescriptionID	AS ProductDescriptionID,
       Culture	AS Culture,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate
       from dw.ProductModelProductDescription as a
left join ods.ProductModelProductDescription as b on
       a.ProductModelID = b.ProductModelID and
       a. ProductDescriptionID = b. ProductDescriptionID and
       a. Culture = b. Culture 
       and b.`year` =  '${YEAR}' and b.`month` = '${MONTH}' and b.`day` = '${DAY}'
where b.ProductModelID  is null
