-- workflow=report_2
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=hive
--  dbName=dw
--  tableName=SalesOrder
-- target=hive
--  dbName=dm
--  tableName=report2
-- writeMode=overwrite
WITH profit_of_each_week AS (SELECT DISTINCT concat(date_format(so.OrderDate,'YYYY'), weekofyear(so.OrderDate))  as  order_date_week,
                                             p.ProductID,
                                             p.Name,
                                             so.UnitPrice * so.OrderQty - p.StandardCost                                                                    AS profit,
                                             DENSE_RANK() OVER (PARTITION BY concat(date_format(so.OrderDate,'YYYY'), weekofyear(so.OrderDate))  ORDER BY so.UnitPrice * so.OrderQty - p.StandardCost DESC) AS `rank`
                             FROM dw.SalesOrder AS so
                                      INNER JOIN dw.Product p ON so.ProductID = p.ProductID)
SELECT order_date_week,
       ProductID,
       Name,
       `rank`,
       profit
FROM profit_of_each_week
WHERE `rank` <= 10
ORDER BY order_date_week, `rank`;