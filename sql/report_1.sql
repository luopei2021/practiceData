-- Report 1: Total due, profit, growth rate of a city in a month.
WITH CurrentTotalDue AS (SELECT EXTRACT(YEAR_MONTH FROM ShipDate)                as ship_date,
                                a.City                                           as city,
                                SUM(DISTINCT TotalDue)                           AS total_due,
                                SUM(so.UnitPrice * so.OrderQty - p.StandardCost) AS profit
                         FROM SalesOrder AS so
                                  INNER JOIN CustomerAddress ca ON so.CustomerID = ca.CustomerID
                                  INNER JOIN Address a ON ca.AddressID = a.AddressID
                                  INNER JOIN Product p ON so.ProductID = p.ProductID
                         GROUP BY EXTRACT(YEAR_MONTH FROM ShipDate), a.City
                         ORDER BY EXTRACT(YEAR_MONTH FROM ShipDate), a.City),
     LastMonthTotalDue AS (SELECT ship_date,
                                  city,
                                  LAG(total_due, 1)
                                      OVER (PARTITION BY ship_date, city ORDER BY ship_date, city) AS last_total_due
                           FROM CurrentTotalDue)

SELECT c.ship_date,
       c.city,
       c.total_due,
       c.profit,
       IF(l.last_total_due IS NULL, 0.0, (c.total_due - l.last_total_due) / l.last_total_due) AS growth_rate
FROM CurrentTotalDue AS c
         INNER JOIN LastMonthTotalDue AS l ON c.ship_date = l.ship_date AND c.city = l.city
ORDER BY ship_date, city;

