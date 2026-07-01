USE [master];
GO

-- Enable preview feature for CES (SQL Server 2025)
ALTER DATABASE [YourSourceDb]
    SET CHANGE_EVENT_STREAMING = ON;
GO


