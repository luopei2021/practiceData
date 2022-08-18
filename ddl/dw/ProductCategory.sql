create table ProductCategory_his
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

create table ProductCategory_curr
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