create table  ProductDescription_his (
ProductDescriptionID int,
Description string,
rowguid string,
ModifiedDate string
)PARTITIONED BY (
    year string,
    month string,
    day string)
STORED AS ORC;


create table  ProductDescription_curr (
ProductDescriptionID int,
Description string,
rowguid string,
ModifiedDate string
)PARTITIONED BY (
    year string,
    month string,
    day string)
STORED AS ORC;