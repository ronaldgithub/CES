-- ============================================================
-- Destination databases for the "Two Consumers (Live)" tab.
-- Two independent consumers each apply the same CES stream to
-- their own copy of Orders, tracked by a private ledger + offset
-- store (the pattern from ces_idempotent.sql).
--
-- WARNING: drops and recreates both databases.
-- ============================================================
USE [master];
GO
DROP DATABASE IF EXISTS CES_Destination1;
DROP DATABASE IF EXISTS CES_Destination2;
GO
CREATE DATABASE CES_Destination1;
CREATE DATABASE CES_Destination2;
GO

USE [CES_Destination1];
GO
CREATE TABLE dbo.Orders (
    OrderID INT PRIMARY KEY CLUSTERED IDENTITY,
    CustomerFirstName NVARCHAR(50),
    CustomerLastName NVARCHAR(50),
    Company NVARCHAR(100),
    SalesDate DATE,
    EstimatedShipDate DATE,
    ShippingID INT,
    ShippingLocation NVARCHAR(100),
    Product NVARCHAR(100),
    Quantity INT,
    Price DECIMAL(10, 2)
);

CREATE TABLE dbo.ces_ledger (
    partition_id        INT          NOT NULL,
    sequence_number     BIGINT       NOT NULL,
    commit_lsn          VARCHAR(64)  NOT NULL,
    processed_at        DATETIME2    NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_ces_ledger PRIMARY KEY (partition_id, sequence_number)
);

CREATE TABLE dbo.ces_offsets (
    partition_id         INT          NOT NULL PRIMARY KEY,
    last_sequence_number BIGINT       NOT NULL,
    last_commit_lsn      VARCHAR(64)  NOT NULL,
    updated_at           DATETIME2    NOT NULL DEFAULT SYSDATETIME()
);
GO

USE [CES_Destination2];
GO
CREATE TABLE dbo.Orders (
    OrderID INT PRIMARY KEY CLUSTERED IDENTITY,
    CustomerFirstName NVARCHAR(50),
    CustomerLastName NVARCHAR(50),
    Company NVARCHAR(100),
    SalesDate DATE,
    EstimatedShipDate DATE,
    ShippingID INT,
    ShippingLocation NVARCHAR(100),
    Product NVARCHAR(100),
    Quantity INT,
    Price DECIMAL(10, 2)
);

CREATE TABLE dbo.ces_ledger (
    partition_id        INT          NOT NULL,
    sequence_number     BIGINT       NOT NULL,
    commit_lsn          VARCHAR(64)  NOT NULL,
    processed_at        DATETIME2    NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_ces_ledger PRIMARY KEY (partition_id, sequence_number)
);

CREATE TABLE dbo.ces_offsets (
    partition_id         INT          NOT NULL PRIMARY KEY,
    last_sequence_number BIGINT       NOT NULL,
    last_commit_lsn      VARCHAR(64)  NOT NULL,
    updated_at           DATETIME2    NOT NULL DEFAULT SYSDATETIME()
);
GO
