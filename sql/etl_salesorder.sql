-- workflow=etl_salesorder
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
--  tableName=SalesOrder
-- target=hive
--  dbName=ods
--  tableName=SalesOrder
-- writeMode=overwrite
-- skipFollowStepWhenEmpty=true
select SalesOrderID	AS SalesOrderID,
       SalesOrderDetailID	AS SalesOrderDetailID,
       RevisionNumber	AS RevisionNumber,
       OrderDate	AS OrderDate,
       DueDate	AS DueDate,
       ShipDate	AS ShipDate,
       Status	AS Status,
       OnlineOrderFlag	AS OnlineOrderFlag,
       SalesOrderNumber	AS SalesOrderNumber,
       PurchaseOrderNumber	AS PurchaseOrderNumber,
       AccountNumber	AS AccountNumber,
       CustomerID	AS CustomerID,
       ShipToAddressID	AS ShipToAddressID,
       BillToAddressID	AS BillToAddressID,
       ShipMethod	AS ShipMethod,
       CreditCardApprovalCode	AS CreditCardApprovalCode,
       SubTotal	AS SubTotal,
       TaxAmt	AS TaxAmt,
       Freight	AS Freight,
       TotalDue	AS TotalDue,
       Comment	AS Comment,
       OrderQty	AS OrderQty,
       ProductID	AS ProductID,
       UnitPrice	AS UnitPrice,
       UnitPriceDiscount	AS UnitPriceDiscount,
       LineTotal	AS LineTotal,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate,
       '${YEAR}' AS "year",
       '${MONTH}' AS "month",
       '${DAY}' AS "day"
from raw.SalesOrder
where ModifiedDate >= '${DATA_RANGE_START}' and ModifiedDate < '${DATA_RANGE_END}'


-- step=3
-- source=hive
--  dbName=ods
--  tableName=SalesOrder
-- target=hive
--  dbName=dw
--  tableName=SalesOrder
-- writeMode=overwrite
select SalesOrderID	AS SalesOrderID,
       SalesOrderDetailID	AS SalesOrderDetailID,
       RevisionNumber	AS RevisionNumber,
       OrderDate	AS OrderDate,
       DueDate	AS DueDate,
       ShipDate	AS ShipDate,
       Status	AS Status,
       OnlineOrderFlag	AS OnlineOrderFlag,
       SalesOrderNumber	AS SalesOrderNumber,
       PurchaseOrderNumber	AS PurchaseOrderNumber,
       AccountNumber	AS AccountNumber,
       CustomerID	AS CustomerID,
       ShipToAddressID	AS ShipToAddressID,
       BillToAddressID	AS BillToAddressID,
       ShipMethod	AS ShipMethod,
       CreditCardApprovalCode	AS CreditCardApprovalCode,
       SubTotal	AS SubTotal,
       TaxAmt	AS TaxAmt,
       Freight	AS Freight,
       TotalDue	AS TotalDue,
       Comment	AS Comment,
       OrderQty	AS OrderQty,
       ProductID	AS ProductID,
       UnitPrice	AS UnitPrice,
       UnitPriceDiscount	AS UnitPriceDiscount,
       LineTotal	AS LineTotal,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate
       from ods.SalesOrder
where `year` =  '${YEAR}' and `month` = '${MONTH}' and `day` = '${DAY}'
union all
select SalesOrderID	AS SalesOrderID,
       SalesOrderDetailID	AS SalesOrderDetailID,
       RevisionNumber	AS RevisionNumber,
       OrderDate	AS OrderDate,
       DueDate	AS DueDate,
       ShipDate	AS ShipDate,
       Status	AS Status,
       OnlineOrderFlag	AS OnlineOrderFlag,
       SalesOrderNumber	AS SalesOrderNumber,
       PurchaseOrderNumber	AS PurchaseOrderNumber,
       AccountNumber	AS AccountNumber,
       CustomerID	AS CustomerID,
       ShipToAddressID	AS ShipToAddressID,
       BillToAddressID	AS BillToAddressID,
       ShipMethod	AS ShipMethod,
       CreditCardApprovalCode	AS CreditCardApprovalCode,
       SubTotal	AS SubTotal,
       TaxAmt	AS TaxAmt,
       Freight	AS Freight,
       TotalDue	AS TotalDue,
       Comment	AS Comment,
       OrderQty	AS OrderQty,
       ProductID	AS ProductID,
       UnitPrice	AS UnitPrice,
       UnitPriceDiscount	AS UnitPriceDiscount,
       LineTotal	AS LineTotal,
       rowguid	AS rowguid,
       ModifiedDate	AS ModifiedDate
       from dw.SalesOrder as a
left join ods.SalesOrder as b on
       a.SalesOrderID = b.SalesOrderID and
       a.SalesOrderDetailID = b.SalesOrderDetailID 
       and b.`year` =  '${YEAR}' and b.`month` = '${MONTH}' and b.`day` = '${DAY}'
where b.SalesOrderID  is null