create table  ProductDescription (
ProductDescriptionID int PRIMARY KEY not null,
Description nvarchar(400) not null ,
rowguid varchar(100) not null,
ModifiedDate datetime not null
);
