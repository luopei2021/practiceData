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
--  tableName=product
-- target=hive
--  dbName=default
--  tableName=product_his
-- writeMode=overwrite
select ProductID	AS ProductID,
       Name	AS Name,
       ProductNumber	AS ProductNumber,
       Color	AS Color,
       StandardCost	AS StandardCost,
       ListPrice	AS ListPrice,
       Size	AS Size,
       Weight	AS Weight,
       ProductCategoryID	AS ProductCategoryID,
       ProductModelID	AS ProductModelID,
       SellStartDate	AS SellStartDate,
       SellEndDate	AS SellEndDate,
       DiscontinuedDate	AS DiscontinuedDate,
       ThumbNailPhoto	AS ThumbNailPhoto,
       ThumbnailPhotoFileName	AS ThumbnailPhotoFileName,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate,
       '${YEAR}' AS "year",
       '${MONTH}' AS "month",
       '${DAY}' AS "day"
from raw.product
where ModifiedDate >= '${DATA_RANGE_START}' and ModifiedDate < '${DATA_RANGE_END}'


-- step=3
-- source=hive
--  dbName=default
--  tableName=product_his
-- target=hive
--  dbName=default
--  tableName=product_curr
-- writeMode=overwrite
select ProductID	AS ProductID,
       Name	AS Name,
       ProductNumber	AS ProductNumber,
       Color	AS Color,
       StandardCost	AS StandardCost,
       ListPrice	AS ListPrice,
       Size	AS Size,
       Weight	AS Weight,
       ProductCategoryID	AS ProductCategoryID,
       ProductModelID	AS ProductModelID,
       SellStartDate	AS SellStartDate,
       SellEndDate	AS SellEndDate,
       DiscontinuedDate	AS DiscontinuedDate,
       ThumbNailPhoto	AS ThumbNailPhoto,
       ThumbnailPhotoFileName	AS ThumbnailPhotoFileName,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate
       from product_his
where `year` =  '${YEAR}' and `month` = '${MONTH}' and `day` = '${DAY}'
union all
select ProductID	AS ProductID,
       Name	AS Name,
       ProductNumber	AS ProductNumber,
       Color	AS Color,
       StandardCost	AS StandardCost,
       ListPrice	AS ListPrice,
       Size	AS Size,
       Weight	AS Weight,
       ProductCategoryID	AS ProductCategoryID,
       ProductModelID	AS ProductModelID,
       SellStartDate	AS SellStartDate,
       SellEndDate	AS SellEndDate,
       DiscontinuedDate	AS DiscontinuedDate,
       ThumbNailPhoto	AS ThumbNailPhoto,
       ThumbnailPhotoFileName	AS ThumbnailPhotoFileName,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate
       from product_curr as a
left join product_his as b on
       a.ProductID = b.ProductID 
       and b.`year` =  '${YEAR}' and b.`month` = '${MONTH}' and b.`day` = '${DAY}'
where b.ProductID  is null