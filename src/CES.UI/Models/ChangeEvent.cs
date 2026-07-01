using System;

namespace CES.UI.Models;

public record ChangeEvent(
    string Operation,     // INS, UPD, DEL
    string Database,
    string Schema,
    string Table,
    string PrimaryKey,    // e.g. "ID:42"
    string CustomerName,  // populated for Orders table
    DateTime ReceivedAt
)
{
    public string TableFullName => $"{Schema}.{Table}";
}
