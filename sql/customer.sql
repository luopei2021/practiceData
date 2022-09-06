-- workflow=customer
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
--  tableName=Customer
-- target=hive
--  dbName=ods
--  tableName=Customer
-- writeMode=overwrite
-- skipFollowStepWhenEmpty=true
select CustomerID	AS CustomerID,
       NameStyle	AS NameStyle,
       Title	AS Title,
       FirstName	AS FirstName,
       MiddleName	AS MiddleName,
       LastName	AS LastName,
       Suffix	AS Suffix,
       CompanyName	AS CompanyName,
       SalesPerson	AS SalesPerson,
       EmailAddress	AS EmailAddress,
       Phone	AS Phone,
       PasswordHash	AS PasswordHash,
       PasswordSalt	AS PasswordSalt,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate,
       '${YEAR}' AS "year",
       '${MONTH}' AS "month",
       '${DAY}' AS "day"
from raw.Customer
where ModifiedDate >= '${DATA_RANGE_START}' and ModifiedDate < '${DATA_RANGE_END}'


-- step=3
-- source=hive
--  dbName=ods
--  tableName=Customer
-- target=hive
--  dbName=dw
--  tableName=Customer
-- writeMode=overwrite
select CustomerID	AS CustomerID,
       NameStyle	AS NameStyle,
       Title	AS Title,
       FirstName	AS FirstName,
       MiddleName	AS MiddleName,
       LastName	AS LastName,
       Suffix	AS Suffix,
       CompanyName	AS CompanyName,
       SalesPerson	AS SalesPerson,
       EmailAddress	AS EmailAddress,
       Phone	AS Phone,
       PasswordHash	AS PasswordHash,
       PasswordSalt	AS PasswordSalt,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate
       from ods.Customer
where `year` =  '${YEAR}' and `month` = '${MONTH}' and `day` = '${DAY}'
union all
select CustomerID	AS CustomerID,
       NameStyle	AS NameStyle,
       Title	AS Title,
       FirstName	AS FirstName,
       MiddleName	AS MiddleName,
       LastName	AS LastName,
       Suffix	AS Suffix,
       CompanyName	AS CompanyName,
       SalesPerson	AS SalesPerson,
       EmailAddress	AS EmailAddress,
       Phone	AS Phone,
       PasswordHash	AS PasswordHash,
       PasswordSalt	AS PasswordSalt,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate
       from dw.Customer as a
left join ods.Customer as b on
       a.CustomerID = b.CustomerID 
       and b.`year` =  '${YEAR}' and b.`month` = '${MONTH}' and b.`day` = '${DAY}'
where b.CustomerID  is null
