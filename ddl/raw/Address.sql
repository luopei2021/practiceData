CREATE TABLE IF NOT EXISTS Address
(
    AddressID     INT                NOT NULL PRIMARY KEY COMMENT 'Primary key for Address records.',
    AddressLine1  NVARCHAR(60)       NOT NULL COMMENT 'First street address line.',
    AddressLine2  NVARCHAR(60) COMMENT 'Second street address line.',
    City          NVARCHAR(30)       NOT NULL COMMENT 'Name of the city.',
    StateProvince NVARCHAR(50)       NOT NULL COMMENT 'Name of state or province.',
    CountryRegion NVARCHAR(50)       NOT NULL COMMENT 'Name of country.',
    PostalCode    NVARCHAR(15)       NOT NULL COMMENT 'Postal code for the street address.',
    rowguid       VARCHAR(100) UNIQUE NOT NULL COMMENT 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    ModifiedDate  datetime           NOT NULL COMMENT 'Date and time the record was last updated.'
);