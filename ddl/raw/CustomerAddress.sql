CREATE TABLE IF NOT EXISTS CustomerAddress
(
    CustomerID   INT                NOT NULL COMMENT 'Primary key. Foreign key to Customer.CustomerID.',
    AddressID    INT                NOT NULL COMMENT 'Primary key. Foreign key to Address.AddressID.',
    AddressType  NVARCHAR(50)       NOT NULL COMMENT 'The kind of Address. One of: Archive, Billing, Home, Main Office, Primary, Shipping',
    rowguid      VARCHAR(100) UNIQUE NOT NULL COMMENT 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    ModifiedDate DATETIME           NOT NULL COMMENT 'Date and time the record was last updated.',
    PRIMARY KEY (CustomerID, AddressID)
);