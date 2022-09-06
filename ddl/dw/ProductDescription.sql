create table  ods.ProductDescription (
ProductDescriptionID int,
Description string,
rowguid string,
ModifiedDate string
)PARTITIONED BY (
    year string,
    month string,
    day string)
STORED AS ORC;


create table  dw.ProductDescription (
ProductDescriptionID int,
Description string,
rowguid string,
ModifiedDate string
)
STORED AS ORC;