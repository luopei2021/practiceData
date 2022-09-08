create table  Product
(
ProductID int
,Name string
,ProductNumber string
,Color string
,StandardCost decimal(32, 10)
,ListPrice decimal(32, 10)
,Size string
,Weight  decimal(32, 10)
,ProductCategoryID int
,ProductModelID int
,SellStartDate string
,SellEndDate string
,DiscontinuedDate string
,ThumbNailPhoto varbinary
,ThumbnailPhotoFileName string
,rowguid string
,ModifiedDate string
)PARTITIONED BY (
    year string,
    month string,
    day string)
STORED AS ORC;

create table  ProductCategory
(
ProductCategoryID     int
,ParentProductCategoryID     int
,Name     string
,rowguid     string
,ModifiedDate string
)PARTITIONED BY (
    year string,
    month string,
    day string)
STORED AS ORC;

create table   ProductDescription
(
ProductDescriptionID int,
Description string,
rowguid string,
ModifiedDate string
)PARTITIONED BY (
    year string,
    month string,
    day string)
STORED AS ORC;

create table  Address
(
AddressID INT
,AddressLine1 STRING
,AddressLine2 STRING
,City STRING
,StateProvince STRING
,CountryRegion STRING
,PostalCode STRING
,rowguid STRING
,ModifiedDate STRING)PARTITIONED BY (
    year STRING,
    month STRING,
    day STRING)
STORED AS ORC;

create table  Customer
(
CustomerID INT
,NameStyle boolean
,Title STRING
,FirstName STRING
,MiddleName STRING
,LastName STRING
,Suffix STRING
,CompanyName STRING
,SalesPerson STRING
,EmailAddress STRING
,Phone STRING
,PasswordHash STRING
,PasswordSalt STRING
,rowguid STRING
,ModifiedDate STRING) PARTITIONED BY (
    year STRING,
    month STRING,
    day STRING)
    STORED AS ORC;

create table  CustomerAddress
(
CustomerID INT
,AddressID INT
,AddressType STRING
,rowguid STRING
,ModifiedDate STRING) PARTITIONED BY (
    year string,
    month string,
    day string)
    STORED AS ORC;

create table  ProductModel
(
ProductModelID INT
,Name STRING
,CatalogDescription STRING
,rowguid STRING
,ModifiedDate STRING) PARTITIONED BY (
    year string,
    month string,
    day string)
    STORED AS ORC;

create table  ProductModelProductDescription
(
ProductModelID INT
,ProductDescriptionID INT
,Culture STRING
,rowguid STRING
,ModifiedDate STRING) PARTITIONED BY (
    year string,
    month string,
    day string)
STORED AS ORC;

create table  SalesOrder
(
SalesOrderID INT
,SalesOrderDetailID INT
,RevisionNumber TINYINT
,OrderDate STRING
,DueDate STRING
,ShipDate STRING
,Status TINYINT
,OnlineOrderFlag boolean
,SalesOrderNumber STRING
,PurchaseOrderNumber STRING
,AccountNumber STRING
,CustomerID INT
,ShipToAddressID INT
,BillToAddressID INT
,ShipMethod STRING
,CreditCardApprovalCode STRING
,SubTotal DOUBLE
,TaxAmt DOUBLE
,Freight DOUBLE
,TotalDue DOUBLE
,Comment STRING
,OrderQty SMALLINT
,ProductID INT
,UnitPrice DOUBLE
,UnitPriceDiscount DOUBLE
,LineTotal DECIMAL(32, 10)
,rowguid STRING
,ModifiedDate STRING) PARTITIONED BY (
    year string,
    month string,
    day string)
    STORED AS ORC;