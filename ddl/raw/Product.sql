create table Product(
ProductID int primary key
,Name nvarchar(50)
,ProductNumber nvarchar(25)
,Color nvarchar(15)
,StandardCost double
,ListPrice double
,Size nvarchar(5)
,Weight decimal(8, 2)
,ProductCategoryID int
,ProductModelID int
,SellStartDate datetime
,SellEndDate datetime
,DiscontinuedDate datetime
,ThumbNailPhoto varbinary(60000)
,ThumbnailPhotoFileName nvarchar(50)
,rowguid varchar(100)
,ModifiedDate datetime
);
