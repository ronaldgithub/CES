using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using CES.UI.Models;
using Microsoft.Data.SqlClient;

namespace CES.UI.Services;

/// <summary>
/// Helpers shared by the live tabs for their local destination databases
/// (CES_Destination1/2, CES_IdempotencyDemo, CES_Batching).
/// </summary>
public static class DestinationDatabase
{
    public static string ConnectionString(string database) =>
        $"Server=localhost;Database={database};Integrated Security=True;TrustServerCertificate=True";

    public static async Task<(List<LedgerEntry> Ledger, OffsetEntry? Offset)> LoadStateAsync(
        string database, CancellationToken cancellationToken)
    {
        await using var conn = new SqlConnection(ConnectionString(database));
        await conn.OpenAsync(cancellationToken);

        var ledger = new List<LedgerEntry>();
        await using (var cmd = new SqlCommand(
            "SELECT partition_id, sequence_number, commit_lsn, processed_at FROM dbo.ces_ledger ORDER BY processed_at", conn))
        await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
                ledger.Add(new LedgerEntry(
                    reader.GetInt32(0), reader.GetInt64(1), reader.GetString(2), reader.GetDateTime(3)));
        }

        OffsetEntry? offset = null;
        await using (var cmd = new SqlCommand(
            "SELECT TOP 1 partition_id, last_sequence_number, last_commit_lsn, updated_at FROM dbo.ces_offsets ORDER BY updated_at DESC", conn))
        await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
        {
            if (await reader.ReadAsync(cancellationToken))
                offset = new OffsetEntry(
                    reader.GetInt32(0), reader.GetInt64(1), reader.GetString(2), reader.GetDateTime(3));
        }

        return (ledger, offset);
    }

    public static async Task ResetAsync(string database, CancellationToken cancellationToken)
    {
        await using var conn = new SqlConnection(ConnectionString(database));
        await conn.OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand(
            "TRUNCATE TABLE dbo.ces_ledger; TRUNCATE TABLE dbo.ces_offsets; DELETE FROM dbo.Orders;", conn);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public static async Task<int> CountOrdersAsync(SqlConnection conn, CancellationToken cancellationToken)
    {
        await using var cmd = new SqlCommand("SELECT COUNT(*) FROM dbo.Orders", conn);
        return (int)(await cmd.ExecuteScalarAsync(cancellationToken))!;
    }
}
