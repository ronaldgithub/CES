-- ============================================================
-- ces_demo.sql — SQL Server 2025 CES demo, end to end
--
-- Everything needed on the SQL Server side of this demo, in the
-- order you run it. Replaces the individual setup scripts.
--
-- Before running, fill in:
--   <your-strong-password>       — master key password
--   <your-sas-primary-key-here>  — Event Hubs RootManageSharedAccessKey
--                                  primary key (Portal: ces-poc-od →
--                                  Shared access policies)
--
-- Azure side (portal or CLI, see README):
--   Event Hubs namespace  : ces-poc-od       (Standard tier)
--   Event hub             : orders
--   Consumer groups       : $Default, consumer1, consumer2, idempotency, batching, multitable
--
-- Parts 1–5 are one-time setup. Part 6 generates test events.
-- Part 7 is diagnostics/recovery — run only when needed.
-- ============================================================


-- ============================================================
-- PART 1 — Source database + Orders table
-- ============================================================
USE [master];
GO
DROP DATABASE IF EXISTS ContosoOrders;
GO
CREATE DATABASE ContosoOrders;
GO
USE [ContosoOrders];
GO

-- CES is a preview feature in SQL Server 2025
ALTER DATABASE SCOPED CONFIGURATION SET PREVIEW_FEATURES = ON;
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
GO

-- Second streamed table for the Multi-Table (Live) tab
CREATE TABLE dbo.OrderLines (
    OrderLineID INT PRIMARY KEY CLUSTERED,
    OrderID INT NOT NULL CONSTRAINT FK_OrderLines_Orders REFERENCES dbo.Orders (OrderID),
    ProductID INT NOT NULL,
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(18, 2) NOT NULL,
    LineTotal AS (Quantity * UnitPrice)
);
GO


-- ============================================================
-- PART 2 — Enable CES + credential for Event Hubs
-- ============================================================
-- Enable Change Event Streaming on this database (idempotent)
EXEC sys.sp_enable_event_stream;
GO

-- The credential secret must be encrypted, which needs a master key
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<your-strong-password>';
GO

-- Credential used by the stream group to authenticate to Event Hubs.
-- IDENTITY = the SAS policy name; SECRET = its primary KEY (the short
-- value, NOT the full connection string).
IF EXISTS (SELECT 1 FROM sys.database_scoped_credentials WHERE name = 'eventhubscred')
    DROP DATABASE SCOPED CREDENTIAL eventhubscred;
GO
CREATE DATABASE SCOPED CREDENTIAL eventhubscred
WITH IDENTITY = 'RootManageSharedAccessKey',
     SECRET   = '<your-sas-primary-key-here>';
GO


-- ============================================================
-- PART 3 — Stream group → Azure Event Hubs
-- ============================================================
-- destination_type is always AzureEventHubsAmqp from SQL Server's
-- side; the Kafka protocol is only used by the consumer app.

-- Drop first so this part is safe to re-run
BEGIN TRY
    EXEC sys.sp_drop_event_stream_group N'OrdersCESGroupKafka';
END TRY
BEGIN CATCH
    -- Group didn't exist yet, nothing to drop
END CATCH
GO

EXEC sys.sp_create_event_stream_group
    @stream_group_name      = N'OrdersCESGroupKafka',
    @destination_type       = N'AzureEventHubsAmqp',
    @destination_location   = N'ces-poc-od.servicebus.windows.net/orders',
    @destination_credential = eventhubscred;
GO

-- Stream the Orders and OrderLines tables into the group
EXEC sys.sp_add_object_to_event_stream_group N'OrdersCESGroupKafka', N'dbo.Orders';
EXEC sys.sp_add_object_to_event_stream_group N'OrdersCESGroupKafka', N'dbo.OrderLines';
GO


-- ============================================================
-- PART 4 — Verify the CES setup
-- ============================================================
EXEC sys.sp_help_change_feed;
GO
EXEC sys.sp_help_change_feed_table @source_schema = 'dbo', @source_name = 'Orders';
GO


-- ============================================================
-- PART 5 — Destination databases for the Two Consumers (Live) tab
-- ============================================================
-- Two independent consumers each apply the same CES stream to their
-- own copy of Orders, tracked by a private ledger + offset store
-- (the pattern from docs/ces_idempotent.sql).
-- CES_IdempotencyDemo is the same schema for the Idempotency &
-- Offsets (Live) tab, which steps through the stream one event at
-- a time. CES_BatchingDemo likewise backs the Batching (Live) tab,
-- which commits whole batches in one transaction. CES_MultiTable
-- backs the Multi-Table (Live) tab and also has OrderLines: one
-- shared ledger/offset guards both tables.
USE [master];
GO
DROP DATABASE IF EXISTS CES_Destination1;
DROP DATABASE IF EXISTS CES_Destination2;
DROP DATABASE IF EXISTS CES_IdempotencyDemo;
DROP DATABASE IF EXISTS CES_BatchingDemo;
DROP DATABASE IF EXISTS CES_MultiTable;
GO
CREATE DATABASE CES_Destination1;
CREATE DATABASE CES_Destination2;
CREATE DATABASE CES_IdempotencyDemo;
CREATE DATABASE CES_BatchingDemo;
CREATE DATABASE CES_MultiTable;
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

USE [CES_IdempotencyDemo];
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

USE [CES_BatchingDemo];
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

USE [CES_MultiTable];
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
-- No FK to Orders here: Orders and OrderLines events can arrive on
-- different partitions, so the apply order is not guaranteed.
-- LineTotal stays computed, exactly like the source table.
CREATE TABLE dbo.OrderLines (
    OrderLineID INT PRIMARY KEY CLUSTERED,
    OrderID INT NOT NULL,
    ProductID INT NOT NULL,
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(18, 2) NOT NULL,
    LineTotal AS (Quantity * UnitPrice)
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


-- ============================================================
-- PART 6 — Generate test events (run with the app watching)
-- ============================================================
-- Each statement below produces one CES event: INS, UPD, DEL.
USE [ContosoOrders];
GO

-- INS
INSERT INTO Orders (CustomerFirstName, CustomerLastName, Company, SalesDate, EstimatedShipDate, ShippingID, ShippingLocation, Product, Quantity, Price)
VALUES ('Charlotte', 'Derksen', 'Space Invaders', '2026-04-20', DATEADD(DAY, 75, '2026-04-20'), 1, 'Apeldoorn', 'Rockets', 1, 11.00);
GO

-- UPD (latest order)
UPDATE Orders
   SET Quantity = 2, Price = 22.00
 WHERE OrderID = (SELECT MAX(OrderID) FROM Orders);
GO

-- DEL (insert a throwaway row, then delete it)
INSERT INTO Orders (CustomerFirstName, CustomerLastName, Company, SalesDate, EstimatedShipDate, ShippingID, ShippingLocation, Product, Quantity, Price)
VALUES ('Test', 'Row', 'Test Company', '2026-01-01', '2026-02-01', 1, 'Nowhere', 'Widget', 1, 1.00);
DELETE FROM Orders WHERE Company = 'Test Company';
GO

-- OrderLines events for the Multi-Table (Live) tab: INS, UPD, DEL
DECLARE @line INT = (SELECT ISNULL(MAX(OrderLineID), 0) + 1 FROM OrderLines);
INSERT INTO OrderLines (OrderLineID, OrderID, ProductID, Quantity, UnitPrice)
VALUES (@line, (SELECT MAX(OrderID) FROM Orders), 9, 2, 11.00);

UPDATE OrderLines SET Quantity = 3 WHERE OrderLineID = @line;

DELETE FROM OrderLines WHERE OrderLineID = @line;
GO


-- ============================================================
-- PART 7 — Diagnostics & recovery (run when something is wrong)
-- ============================================================
-- Events not arriving? Check the CES error log:
USE [ContosoOrders];
SELECT TOP 10 entry_time, error_number, error_message
FROM sys.dm_change_feed_errors
ORDER BY entry_time DESC;
GO

-- 'InvalidSignature: The token has an invalid signature' means the
-- Event Hubs SAS key changed (e.g. the namespace was recreated).
-- Fix in TWO steps — updating the credential alone is NOT enough,
-- because CES caches the signed token until the stream group restarts:
/*
-- Step 1: new key into the credential
ALTER DATABASE SCOPED CREDENTIAL eventhubscred
WITH IDENTITY = 'RootManageSharedAccessKey',
     SECRET   = '<new-primary-key>';

-- Step 2: recreate the stream group to force a fresh token
EXEC sys.sp_drop_event_stream_group N'OrdersCESGroupKafka';
EXEC sys.sp_create_event_stream_group
    @stream_group_name      = N'OrdersCESGroupKafka',
    @destination_type       = N'AzureEventHubsAmqp',
    @destination_location   = N'ces-poc-od.servicebus.windows.net/orders',
    @destination_credential = eventhubscred;
EXEC sys.sp_add_object_to_event_stream_group N'OrdersCESGroupKafka', N'dbo.Orders';
EXEC sys.sp_add_object_to_event_stream_group N'OrdersCESGroupKafka', N'dbo.OrderLines';
*/
GO
