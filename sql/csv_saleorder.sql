-- workflow=csv_saleorder
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=0
-- source=temp
-- target=variables
select from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'yyyy')                                as `YEAR`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'MM')                                  as `MONTH`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'dd')                                  as `DAY`;

-- stepId=3
-- source=transformation
--  className=com.thoughtworks.bigdata.etl.spark.transformation.FileCleanTransformer
--  methodName=transform
--  transformerType=object
--  filePath=hdfs://master:9000/etl/data/
--  fileNamePattern=sale_order.csv
-- targetConfig=do_nothing

-- step=1
-- source=sftp
--  configPrefix=order
--  fileNamePattern=\w*.csv
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



-- step=5
-- source=temp
--  tableName=`tmp_sale_order`
-- target=hive
--  dbName=ods
--  tableName=csv_SalesOrder
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
from `tmp_sale_order`


-- step=6
-- source=hive
--  dbName=ods
--  tableName=csv_SalesOrder
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
select a.SalesOrderID	AS SalesOrderID,
       a.SalesOrderDetailID	AS SalesOrderDetailID,
       a.RevisionNumber	AS RevisionNumber,
       a.OrderDate	AS OrderDate,
       a.DueDate	AS DueDate,
       a.ShipDate	AS ShipDate,
       a.Status	AS Status,
       a.OnlineOrderFlag	AS OnlineOrderFlag,
       a.SalesOrderNumber	AS SalesOrderNumber,
       a.PurchaseOrderNumber	AS PurchaseOrderNumber,
       a.AccountNumber	AS AccountNumber,
       a.CustomerID	AS CustomerID,
       a.ShipToAddressID	AS ShipToAddressID,
       a.BillToAddressID	AS BillToAddressID,
       a.ShipMethod	AS ShipMethod,
       a.CreditCardApprovalCode	AS CreditCardApprovalCode,
       a.SubTotal	AS SubTotal,
       a.TaxAmt	AS TaxAmt,
       a.Freight	AS Freight,
       a.TotalDue	AS TotalDue,
       a.Comment	AS Comment,
       a.OrderQty	AS OrderQty,
       a.ProductID	AS ProductID,
       a.UnitPrice	AS UnitPrice,
       a.UnitPriceDiscount	AS UnitPriceDiscount,
       a.LineTotal	AS LineTotal,
       a.rowguid	AS rowguid,
       a.ModifiedDate	AS ModifiedDate
       from dw.SalesOrder as a
left join ods.csv_SalesOrder as b on
       a.SalesOrderID = b.SalesOrderID and
       a.SalesOrderDetailID = b.SalesOrderDetailID
       and b.`year` =  '${YEAR}' and b.`month` = '${MONTH}' and b.`day` = '${DAY}'
where b.SalesOrderID  is null
