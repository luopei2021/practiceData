-- workflow=Address
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
--  tableName=Address
-- target=hive
--  dbName=ods
--  tableName=Address
-- writeMode=overwrite
-- skipFollowStepWhenEmpty=true
select AddressID	AS AddressID,
       AddressLine1	AS AddressLine1,
       AddressLine2	AS AddressLine2,
       City	AS City,
       StateProvince	AS StateProvince,
       CountryRegion	AS CountryRegion,
       PostalCode	AS PostalCode,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate,
       '${YEAR}' AS "year",
       '${MONTH}' AS "month",
       '${DAY}' AS "day"
from raw.Address
where ModifiedDate >= '${DATA_RANGE_START}' and ModifiedDate < '${DATA_RANGE_END}'


-- step=3
-- source=hive
--  dbName=ods
--  tableName=Address
-- target=hive
--  dbName=dw
--  tableName=Address
-- writeMode=overwrite
select AddressID	AS AddressID,
       AddressLine1	AS AddressLine1,
       AddressLine2	AS AddressLine2,
       City	AS City,
       StateProvince	AS StateProvince,
       CountryRegion	AS CountryRegion,
       PostalCode	AS PostalCode,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate
       from ods.Address
where `year` =  '${YEAR}' and `month` = '${MONTH}' and `day` = '${DAY}'
union all
select AddressID	AS AddressID,
       AddressLine1	AS AddressLine1,
       AddressLine2	AS AddressLine2,
       City	AS City,
       StateProvince	AS StateProvince,
       CountryRegion	AS CountryRegion,
       PostalCode	AS PostalCode,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate
       from dw.Address as a
left join ods.Address as b on
       a.AddressID = b.AddressID 
       and b.`year` =  '${YEAR}' and b.`month` = '${MONTH}' and b.`day` = '${DAY}'
where b.AddressID  is null