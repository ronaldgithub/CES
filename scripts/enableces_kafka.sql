-- ============================================================
-- CES → Azure Event Hubs (Kafka protocol)
-- Namespace : ces-poc.servicebus.windows.net
-- Event Hub : orders
--
-- Run AFTER orders_ddl.sql has been executed once.
-- Safe to re-run (drops group first if it exists).
-- ============================================================
USE [ContosoOrders];
GO

-- Enable CES on this instance (idempotent)
EXEC sys.sp_enable_event_stream;
GO

-- Create/recreate the credential for Event Hubs auth
IF EXISTS (SELECT 1 FROM sys.database_scoped_credentials WHERE name = 'eventhubscred')
    DROP DATABASE SCOPED CREDENTIAL eventhubscred;
GO
CREATE DATABASE SCOPED CREDENTIAL eventhubscred
WITH IDENTITY = 'RootManageSharedAccessKey',
     SECRET   = '<your-sas-primary-key-here>';
GO

-- Drop existing stream group if re-running
BEGIN TRY
    EXEC sys.sp_drop_event_stream_group N'OrdersCESGroupKafka';
END TRY
BEGIN CATCH
    -- Group didn't exist yet, nothing to drop
END CATCH
GO

-- Create stream group → Azure Event Hubs (AMQP from SQL Server side)
-- The Avalonia app reads via the Kafka-compatible endpoint on the consumer side
EXEC sys.sp_create_event_stream_group
    @stream_group_name      = N'OrdersCESGroupKafka',
    @destination_type       = N'AzureEventHubsAmqp',
    @destination_location   = N'ces-poc.servicebus.windows.net/orders',
    @destination_credential = eventhubscred;
GO

-- Add the Orders table to the group
EXEC sys.sp_add_object_to_event_stream_group N'OrdersCESGroupKafka', N'dbo.Orders';
GO

-- Verify
EXEC sys.sp_help_change_feed;
GO
EXEC sys.sp_help_change_feed_table @source_schema = 'dbo', @source_name = 'Orders';
GO
