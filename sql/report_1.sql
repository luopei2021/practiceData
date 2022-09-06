-- workflow=report_1
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=hive
--  dbName=dw
--  tableName=SalesOrder
-- target=hive
--  dbName=dm
--  tableName=report1
-- writeMode=overwrite
WITH CurrentTotalDue AS (SELECT date_format(ShipDate,'YYYY-MM-01')               as ship_date,
                                a.City                                           as city,
                                SUM(DISTINCT TotalDue)                           AS total_due,
                                SUM(so.UnitPrice * so.OrderQty - p.StandardCost) AS profit
                         FROM dw.SalesOrder AS so
                                  INNER JOIN dw.CustomerAddress ca ON so.CustomerID = ca.CustomerID
                                  INNER JOIN dw.Address a ON ca.AddressID = a.AddressID
                                  INNER JOIN dw.Product p ON so.ProductID = p.ProductID
                         GROUP BY date_format(ShipDate,'YYYY-MM-01') , a.City
                         ORDER BY date_format(ShipDate,'YYYY-MM-01') , a.City),
     LastMonthTotalDue AS (SELECT ship_date,
                                  city,
                                  LAG(total_due, 1)
                                      OVER (PARTITION BY ship_date, city ORDER BY ship_date, city) AS last_total_due
                           FROM CurrentTotalDue)

SELECT c.ship_date as report_date,
       c.city,
       c.total_due,
       c.profit,
       IF(l.last_total_due IS NULL, 0.0, (c.total_due - l.last_total_due) / l.last_total_due) AS growth_rate
FROM CurrentTotalDue AS c
         INNER JOIN LastMonthTotalDue AS l ON c.ship_date = l.ship_date AND c.city = l.city
ORDER BY report_date, city;
