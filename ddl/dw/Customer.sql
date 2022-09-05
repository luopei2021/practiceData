CREATE TABLE ods.Customer
(
    CustomerID   INT,
    NameStyle    BINARY,
    Title        STRING,
    FirstName    STRING,
    MiddleName   STRING,
    LastName     STRING,
    Suffix       STRING,
    CompanyName  STRING,
    SalesPerson  STRING,
    EmailAddress STRING,
    Phone        STRING,
    PasswordHash STRING,
    PasswordSalt STRING,
    rowguid      STRING,
    ModifiedDate STRING
) PARTITIONED BY (
    year STRING,
    month STRING,
    day STRING)
    STORED AS ORC;

CREATE TABLE dw.Customer
(
    CustomerID   INT,
    NameStyle    BINARY,
    Title        STRING,
    FirstName    STRING,
    MiddleName   STRING,
    LastName     STRING,
    Suffix       STRING,
    CompanyName  STRING,
    SalesPerson  STRING,
    EmailAddress STRING,
    Phone        STRING,
    PasswordHash STRING,
    PasswordSalt STRING,
    rowguid      STRING,
    ModifiedDate STRING
) PARTITIONED BY (
    year STRING,
    month STRING,
    day STRING)
    STORED AS ORC;