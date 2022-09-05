CREATE TABLE ods.ProductModel
(
    ProductModelID     INT,
    Name               STRING,
    CatalogDescription STRING,
    rowguid            STRING,
    ModifiedDate       STRING
) PARTITIONED BY (
    year string,
    month string,
    day string)
    STORED AS ORC;

CREATE TABLE dw.ProductModel
(
    ProductModelID     INT,
    Name               STRING,
    CatalogDescription STRING,
    rowguid            STRING,
    ModifiedDate       STRING
) PARTITIONED BY (
    year string,
    month string,
    day string)
    STORED AS ORC;