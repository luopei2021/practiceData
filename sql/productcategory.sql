-- workflow=productcategory
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
--  tableName=ProductCategory
-- target=hive
--  dbName=ods
--  tableName=ProductCategory
-- writeMode=overwrite
-- skipFollowStepWhenEmpty=true
select ProductCategoryID	AS ProductCategoryID,
       ParentProductCategoryID	AS ParentProductCategoryID,
       Name	AS Name,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate,
       '${YEAR}' AS "year",
       '${MONTH}' AS "month",
       '${DAY}' AS "day"
from raw.ProductCategory
where ModifiedDate >= '${DATA_RANGE_START}' and ModifiedDate < '${DATA_RANGE_END}'


-- step=3
-- source=hive
--  dbName=ods
--  tableName=ProductCategory
-- target=hive
--  dbName=dw
--  tableName=ProductCategory
-- writeMode=overwrite
select ProductCategoryID	AS ProductCategoryID,
       ParentProductCategoryID	AS ParentProductCategoryID,
       Name	AS Name,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate
       from ods.ProductCategory
where `year` =  '${YEAR}' and `month` = '${MONTH}' and `day` = '${DAY}'
union all
select ProductCategoryID	AS ProductCategoryID,
       ParentProductCategoryID	AS ParentProductCategoryID,
       Name	AS Name,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate
       from dw.ProductCategory as a
left join ods.ProductCategory as b on
       a.ProductCategoryID = b.ProductCategoryID 
       and b.`year` =  '${YEAR}' and b.`month` = '${MONTH}' and b.`day` = '${DAY}'
where b.ProductCategoryID  is null
