-- Top 10 product profit for a week, the week starts on Monday
WITH profit_of_each_week AS (SELECT DISTINCT YEARWEEK(so.OrderDate, 1)                                                                                      AS order_date_week,
                                             p.ProductID,
                                             p.Name,
                                             so.UnitPrice * so.OrderQty - p.StandardCost                                                                    AS profit,
                                             DENSE_RANK() OVER (PARTITION BY YEARWEEK(so.OrderDate, 1) ORDER BY so.UnitPrice * so.OrderQty - p.StandardCost DESC) AS `rank`
                             FROM SalesOrder AS so
                                      INNER JOIN Product p ON so.ProductID = p.ProductID)
SELECT order_date_week,
       ProductID,
       Name,
       profit
FROM profit_of_each_week
WHERE `rank` <= 10
ORDER BY order_date_week, `rank`;