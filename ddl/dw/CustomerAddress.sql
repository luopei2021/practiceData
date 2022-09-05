CREATE TABLE ods.CustomerAddress
(
    CustomerID   INT,
    AddressID    INT,
    AddressType  STRING,
    rowguid      STRING,
    ModifiedDate STRING
) PARTITIONED BY (
    year string,
    month string,
    day string)
    STORED AS ORC;

CREATE TABLE dw.CustomerAddress
(
    CustomerID   INT,
    AddressID    INT,
    AddressType  STRING,
    rowguid      STRING,
    ModifiedDate STRING
) PARTITIONED BY (
    year string,
    month string,
    day string)
    STORED AS ORC;