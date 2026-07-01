namespace CES.UI.Models;

public record SimulatedEvent(
    int PartitionId,
    long SequenceNumber,
    string CommitLsn,
    string Operation,     // INS, UPD, DEL
    string Schema,
    string Table,
    string PrimaryKey,    // e.g. "OrderID=1"
    string Payload        // short human-readable description, e.g. "Status='Shipped'"
);
