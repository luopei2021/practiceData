
create table ProductCategory(
ProductCategoryID     int primary key
,ParentProductCategoryID     int
,Name     nvarchar(50)
,rowguid     varchar(100)
,ModifiedDate datetime
);