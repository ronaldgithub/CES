USE [YourSourceDb];
GO

-- Create a streaming group that points to Azure Event Hubs
CREATE EVENT STREAM GROUP [OrdersCesGroup]
WITH
(
    DESTINATION = 'AZURE_EVENT_HUBS',
    DESTINATION_ENDPOINT = 'sb://<your-namespace>.servicebus.windows.net/',
    DESTINATION_ENTITY_PATH = '<your-event-hub-name>',
    AUTHENTICATION = 'SAS',  -- or 'ENTRA' when you wire up Entra ID
    AUTHENTICATION_SECRET = '<your-SAS-or-credential>',
    MAX_MESSAGE_SIZE_BYTES = 256000,
    PARTITIONING_SCHEME = 'HASH',
    PARTITION_KEY = 'OrderID'
);
GO
