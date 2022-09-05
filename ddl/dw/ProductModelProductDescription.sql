CREATE TABLE ods.ProductModelProductDescription
(
    ProductModelID       INT,
    ProductDescriptionID INT,
    Culture              STRING,
    rowguid              STRING,
    ModifiedDate         STRING
) PARTITIONED BY (
    year string,
    month string,
    day string)
STORED AS ORC;

CREATE TABLE dw.ProductModelProductDescription
(
    ProductModelID       INT,
    ProductDescriptionID INT,
    Culture              STRING,
    rowguid              STRING,
    ModifiedDate         STRING
) PARTITIONED BY (
    year string,
    month string,
    day string)
STORED AS ORC;