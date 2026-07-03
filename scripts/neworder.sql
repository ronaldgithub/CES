USE [ContosoOrders];
GO

-- Insert a new Order
INSERT INTO Orders (CustomerFirstName, CustomerLastName, Company, SalesDate, EstimatedShipDate, ShippingID, ShippingLocation, Product, Quantity, Price)
VALUES 
('Charlotte', 'Derksen', 'Space Invaders', '2026-04-20'
, DATEADD(DAY, 75, '2025-04-20'), 1, 'Apeldoorn', 'Rockets', 1, 11.00);
GO


