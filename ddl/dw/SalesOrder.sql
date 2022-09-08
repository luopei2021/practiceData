drop table  ods.SalesOrder;
drop table  dw.SalesOrder;
drop table  ods.csv_SalesOrder;
CREATE TABLE ods.SalesOrder
(
    SalesOrderID           INT,
    SalesOrderDetailID     INT,
    RevisionNumber         TINYINT,
    OrderDate              STRING,
    DueDate                STRING,
    ShipDate               STRING,
    Status                 TINYINT,
    OnlineOrderFlag        boolean,
    SalesOrderNumber       STRING,
    PurchaseOrderNumber    STRING,
    AccountNumber          STRING,
    CustomerID             INT,
    ShipToAddressID        INT,
    BillToAddressID        INT,
    ShipMethod             STRING,
    CreditCardApprovalCode STRING,
    SubTotal               DOUBLE,
    TaxAmt                 DOUBLE,
    Freight                DOUBLE,
    TotalDue               DOUBLE,
    Comment                STRING,
    OrderQty               SMALLINT,
    ProductID              INT,
    UnitPrice              DOUBLE,
    UnitPriceDiscount      DOUBLE,
    LineTotal              DECIMAL(32, 10),
    rowguid                STRING,
    ModifiedDate           STRING
) PARTITIONED BY (
    year string,
    month string,
    day string)
    STORED AS ORC;

CREATE TABLE dw.SalesOrder
(
    SalesOrderID           INT,
    SalesOrderDetailID     INT,
    RevisionNumber         TINYINT,
    OrderDate              STRING,
    DueDate                STRING,
    ShipDate               STRING,
    Status                 TINYINT,
    OnlineOrderFlag        boolean,
    SalesOrderNumber       STRING,
    PurchaseOrderNumber    STRING,
    AccountNumber          STRING,
    CustomerID             INT,
    ShipToAddressID        INT,
    BillToAddressID        INT,
    ShipMethod             STRING,
    CreditCardApprovalCode STRING,
    SubTotal               DOUBLE,
    TaxAmt                 DOUBLE,
    Freight                DOUBLE,
    TotalDue               DOUBLE,
    Comment                STRING,
    OrderQty               SMALLINT,
    ProductID              INT,
    UnitPrice              DOUBLE,
    UnitPriceDiscount      DOUBLE,
    LineTotal              DECIMAL(32, 10),
    rowguid                STRING,
    ModifiedDate           STRING
)
    STORED AS ORC;

CREATE TABLE ods.csv_SalesOrder
(
    SalesOrderID           INT,
    SalesOrderDetailID     INT,
    RevisionNumber         TINYINT,
    OrderDate              STRING,
    DueDate                STRING,
    ShipDate               STRING,
    Status                 TINYINT,
    OnlineOrderFlag        boolean,
    SalesOrderNumber       STRING,
    PurchaseOrderNumber    STRING,
    AccountNumber          STRING,
    CustomerID             INT,
    ShipToAddressID        INT,
    BillToAddressID        INT,
    ShipMethod             STRING,
    CreditCardApprovalCode STRING,
    SubTotal               DOUBLE,
    TaxAmt                 DOUBLE,
    Freight                DOUBLE,
    TotalDue               DOUBLE,
    Comment                STRING,
    OrderQty               SMALLINT,
    ProductID              INT,
    UnitPrice              DOUBLE,
    UnitPriceDiscount      DOUBLE,
    LineTotal              DECIMAL(32, 10),
    rowguid                STRING,
    ModifiedDate           STRING
) PARTITIONED BY (
    year string,
    month string,
    day string)
    STORED AS ORC;