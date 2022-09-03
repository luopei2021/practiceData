CREATE TABLE IF NOT EXISTS ProductModelProductDescription
(
    ProductModelID       INT          NOT NULL COMMENT 'Primary key. Foreign key to ProductModel.ProductModelID.',
    ProductDescriptionID INT          NOT NULL COMMENT 'Primary key. Foreign key to ProductDescription.ProductDescriptionID.',
    Culture              NCHAR(6)     NOT NULL COMMENT 'The culture for which the description is written',
    rowguid              VARCHAR(100) UNIQUE NOT NULL,
    ModifiedDate         DATETIME     NOT NULL COMMENT 'Date and time the record was last updated.',
    PRIMARY KEY (ProductModelID, ProductDescriptionID, Culture)
);