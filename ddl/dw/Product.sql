create table product_his
(
`ProductID` int
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

create table product_curr(
`ProductID` int
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
