CREATE TABLE IF NOT EXISTS Customer
(
    CustomerID   INT                NOT NULL PRIMARY KEY COMMENT 'Primary key for Customer records.',
    NameStyle    BIT                NOT NULL COMMENT '0 = The data in FirstName and LastName are stored in western style (first name, last name) order.  1 = Eastern style (last name, first name) order.',
    Title        NVARCHAR(8) COMMENT 'A courtesy title. For example, Mr. or Ms.',
    FirstName    NVARCHAR(50)       NOT NULL COMMENT 'First name of the person.',
    MiddleName   NVARCHAR(50) COMMENT 'Middle name or middle initial of the person.',
    LastName     NVARCHAR(50)       NOT NULL COMMENT 'Last name of the person.',
    Suffix       NVARCHAR(10) COMMENT 'Surname suffix. For example, Sr. or Jr.',
    CompanyName  NVARCHAR(128) COMMENT 'The customer\'s organization.',
    SalesPerson  NVARCHAR(256) COMMENT 'The customer\'s sales person, an employee of AdventureWorks Cycles.',
    EmailAddress NVARCHAR(50) COMMENT 'E-mail address for the person.',
    Phone        NVARCHAR(25) COMMENT 'Phone number associated with the person.',
    PasswordHash VARCHAR(128)       NOT NULL COMMENT 'Password for the e-mail account.',
    PasswordSalt VARCHAR(10)        NOT NULL COMMENT 'Random value concatenated with the password string before the password is hashed.',
    rowguid      VARCHAR(100) UNIQUE NOT NULL COMMENT 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    ModifiedDate DATETIME           NOT NULL COMMENT 'Date and time the record was last updated.'
);