create table ods.product
(
`ProductID` int
,Name string
,ProductNumber string
,Color string
,StandardCost decimal(32, 10)
,ListPrice decimal(32, 10)
,`Size` string
,Weight  decimal(32, 10)
,ProductCategoryID int
,ProductModelID int
,SellStartDate string
,SellEndDate string
,DiscontinuedDate string
,ThumbNailPhoto binary
,ThumbnailPhotoFileName string
,rowguid string
,ModifiedDate string
)PARTITIONED BY (
    year string,
    month string,
    day string)
STORED AS ORC;

create table dw.product(
`ProductID` int
,Name string
,ProductNumber string
,Color string
,StandardCost decimal(32, 10)
,ListPrice decimal(32, 10)
,`Size` string
,Weight  decimal(32, 10)
,ProductCategoryID int
,ProductModelID int
,SellStartDate string
,SellEndDate string
,DiscontinuedDate string
,ThumbNailPhoto binary
,ThumbnailPhotoFileName string
,rowguid string
,ModifiedDate string
)
STORED AS ORC;
