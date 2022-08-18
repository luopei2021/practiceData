create table ods.ProductCategory
(
ProductCategoryID     int
,ParentProductCategoryID     int
,Name     string
,rowguid     sting
,ModifiedDate string
)PARTITIONED BY (
    year string,
    month string,
    day string)
STORED AS ORC;

create table dw.ProductCategory
(
ProductCategoryID     int
,ParentProductCategoryID     int
,Name     string
,rowguid     sting
,ModifiedDate string
)PARTITIONED BY (
    year string,
    month string,
    day string)
STORED AS ORC;