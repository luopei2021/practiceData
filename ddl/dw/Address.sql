CREATE TABLE ods.Address
(
    AddressID     INT,
    AddressLine1  STRING,
    AddressLine2  STRING,
    City          STRING,
    StateProvince STRING,
    CountryRegion STRING,
    PostalCode    STRING,
    rowguid       STRING,
    ModifiedDate  STRING
) PARTITIONED BY (
    year STRING,
    month STRING,
    day STRING)
    STORED AS ORC;

CREATE TABLE dw.Address
(
    AddressID     INT,
    AddressLine1  STRING,
    AddressLine2  STRING,
    City          STRING,
    StateProvince STRING,
    CountryRegion STRING,
    PostalCode    STRING,
    rowguid       STRING,
    ModifiedDate  STRING
) PARTITIONED BY (
    year STRING,
    month STRING,
    day STRING)
    STORED AS ORC;