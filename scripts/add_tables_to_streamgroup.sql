USE [YourSourceDb];
GO

ALTER EVENT STREAM GROUP [OrdersCesGroup]
ADD TABLE [dbo].[Orders]
WITH
(
    PRIMARY_KEY = 'OrderID',
    INCLUDE_COLUMNS = 'CustomerID, OrderDate, Status, TotalAmount'
);
GO

ALTER EVENT STREAM GROUP [OrdersCesGroup]
ADD TABLE [dbo].[OrderLines]
WITH
(
    PRIMARY_KEY = 'OrderLineID',
    INCLUDE_COLUMNS = 'OrderID, ProductID, Quantity, UnitPrice'
);
GO
