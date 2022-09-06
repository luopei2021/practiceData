-- workflow=product
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
--  tableName=Product
-- target=hive
--  dbName=ods
--  tableName=Product
-- writeMode=overwrite
-- skipFollowStepWhenEmpty=true
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
from raw.Product
where ModifiedDate >= '${DATA_RANGE_START}' and ModifiedDate < '${DATA_RANGE_END}'


-- step=3
-- source=hive
--  dbName=ods
--  tableName=Product
-- target=hive
--  dbName=dw
--  tableName=Product
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
       from ods.Product
where `year` =  '${YEAR}' and `month` = '${MONTH}' and `day` = '${DAY}'
union all
select a.ProductID	AS ProductID,
       a.Name	AS Name,
       a.ProductNumber	AS ProductNumber,
       a.Color	AS Color,
       a.StandardCost	AS StandardCost,
       a.ListPrice	AS ListPrice,
       a.Size	AS Size,
       a.Weight	AS Weight,
       a.ProductCategoryID	AS ProductCategoryID,
       a.ProductModelID	AS ProductModelID,
       a.SellStartDate	AS SellStartDate,
       a.SellEndDate	AS SellEndDate,
       a.DiscontinuedDate	AS DiscontinuedDate,
       a.ThumbNailPhoto	AS ThumbNailPhoto,
       a.ThumbnailPhotoFileName	AS ThumbnailPhotoFileName,
       a.rowguid	AS rowguid,
       a.ModifiedDate	AS ModifiedDate
       from dw.Product as a
left join ods.Product as b on
       a.ProductID = b.ProductID 
       and b.`year` =  '${YEAR}' and b.`month` = '${MONTH}' and b.`day` = '${DAY}'
where b.ProductID  is null
