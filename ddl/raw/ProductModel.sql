CREATE TABLE IF NOT EXISTS ProductModel
(
    ProductModelID     INT NOT NULL PRIMARY KEY,
    Name               NVARCHAR(50) NOT NULL,
    CatalogDescription TEXT,
    rowguid            VARCHAR(100) UNIQUE NOT NULL,
    ModifiedDate       DATETIME NOT NULL COMMENT 'Date and time the record was last updated.'
)