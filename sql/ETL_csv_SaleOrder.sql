-- workflow=csv_SaleOrder
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=sftp
--  configPrefix=order
--  fileNamePattern=\w*.txt
--  sourceDir=/home/sz03/saleOrderData
--  tempDestinationDir=/tmp/order/
--  hdfsDir=hdfs://master:9000/etl/
-- target=variables

-- stepId=2
-- source=csv
--  encoding=utf-8
--  sep=\t
--  quote='
--  escape=\
--  header=true
--  inferSchema=true
--  fileNamePattern=sale_order.csv
--  fileDir=hdfs://master:9000/etl/
-- target=temp
--  tableName=`tmp_sale_order`

-- step=3
-- source=mysql
--  dbName=raw
--  tableName=SalesOrder
-- target=hive
--  dbName=ods
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
       ModifiedDate	AS ModifiedDate,
       '${YEAR}' AS "year",
       '${MONTH}' AS "month",
       '${DAY}' AS "day"
from raw.SalesOrder
where ModifiedDate >= '${DATA_RANGE_START}' and ModifiedDate < '${DATA_RANGE_END}'


-- step=4
-- source=temp
--  tableName=`tmp_sale_order`
-- target=hive
--  dbName=ods
--  tableName=SalesOrder
-- writeMode=append
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
from `tmp_sale_order`



-- step=5
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
       from SalesOrder_his
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