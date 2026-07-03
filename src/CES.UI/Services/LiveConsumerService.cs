using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Avalonia.Threading;
using CES.UI.ViewModels;
using Confluent.Kafka;
using Microsoft.Data.SqlClient;

namespace CES.UI.Services;

/// <summary>
/// A real CES consumer: reads the orders Event Hub through its own consumer group
/// and applies each event to a destination database using the ledger + offset
/// pattern from docs/ces_idempotent.sql.
///
/// Kafka offsets are deliberately never committed (EnableAutoCommit = false), so
/// every Start replays the stream from the beginning — the ces_ledger table is
/// what makes that replay safe.
/// </summary>
public class LiveConsumerService(LiveConsumerState state)
{
    private const string BootstrapServers = "ces-poc.servicebus.windows.net:9093";
    private const string Topic = "orders";

    private static readonly string EventHubsConnectionString =
        Environment.GetEnvironmentVariable("CES_CONNECTION_STRING")
        ?? throw new InvalidOperationException(
            "Set the CES_CONNECTION_STRING environment variable to your Event Hubs connection string.");

    private static string SqlConnectionString(string database) =>
        $"Server=localhost;Database={database};Integrated Security=True;TrustServerCertificate=True";

    public void Start(CancellationToken cancellationToken) =>
        Task.Run(() => ConsumeLoop(cancellationToken), cancellationToken);

    private async Task ConsumeLoop(CancellationToken cancellationToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = state.ConsumerGroup,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,   // always replay from the start — the ledger dedups
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = "$ConnectionString",
            SaslPassword = EventHubsConnectionString
        };

        try
        {
            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe(Topic);

            await using var conn = new SqlConnection(SqlConnectionString(state.DatabaseName));
            await conn.OpenAsync(cancellationToken);

            Dispatcher.UIThread.Post(() =>
                state.Status = $"Connected · group {state.ConsumerGroup} → {state.DatabaseName}");

            while (!cancellationToken.IsCancellationRequested)
            {
                ConsumeResult<Ignore, string>? result;
                try
                {
                    result = consumer.Consume(cancellationToken);
                }
                catch (ConsumeException ex) when (!cancellationToken.IsCancellationRequested)
                {
                    Dispatcher.UIThread.Post(() => state.Status = $"Consume error: {ex.Error.Reason}");
                    continue;
                }

                if (result?.Message?.Value is null) continue;

                var ev = ParseEvent(result.Message.Value);
                if (ev is null || ev.Table != "Orders") continue;

                var partitionId = result.Partition.Value;
                var sequenceNumber = result.Offset.Value;

                var applied = await ApplyAsync(conn, ev, partitionId, sequenceNumber, cancellationToken);

                Dispatcher.UIThread.Post(() =>
                    state.RecordResult(ev.Operation, ev.OrderId, sequenceNumber, ev.CommitLsn, applied));
            }

            consumer.Close();
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Dispatcher.UIThread.Post(() => state.MarkStopped($"Failed: {ex.Message}"));
            return;
        }

        Dispatcher.UIThread.Post(() => state.MarkStopped("Stopped"));
    }

    // Idempotency-check-then-apply in a single transaction, exactly as designed in
    // docs/ces_idempotent.sql: ledger check → DML → ledger insert → offset upsert.
    private static async Task<bool> ApplyAsync(
        SqlConnection conn, ParsedOrderEvent ev, int partitionId, long sequenceNumber,
        CancellationToken cancellationToken)
    {
        const string sql = """
            SET XACT_ABORT ON;
            DECLARE @applied bit = 0;
            BEGIN TRAN;

            IF NOT EXISTS (SELECT 1 FROM dbo.ces_ledger WITH (UPDLOCK, HOLDLOCK)
                           WHERE partition_id = @PartitionId AND sequence_number = @SequenceNumber)
            BEGIN
                IF @Operation IN ('INS','UPD')
                BEGIN
                    SET IDENTITY_INSERT dbo.Orders ON;
                    MERGE dbo.Orders AS T
                    USING (VALUES (@OrderID, @CustomerFirstName, @CustomerLastName, @Company, @SalesDate,
                                   @EstimatedShipDate, @ShippingID, @ShippingLocation, @Product, @Quantity, @Price))
                          AS S(OrderID, CustomerFirstName, CustomerLastName, Company, SalesDate,
                               EstimatedShipDate, ShippingID, ShippingLocation, Product, Quantity, Price)
                    ON T.OrderID = S.OrderID
                    WHEN MATCHED THEN UPDATE SET
                        CustomerFirstName = S.CustomerFirstName,
                        CustomerLastName  = S.CustomerLastName,
                        Company           = S.Company,
                        SalesDate         = S.SalesDate,
                        EstimatedShipDate = S.EstimatedShipDate,
                        ShippingID        = S.ShippingID,
                        ShippingLocation  = S.ShippingLocation,
                        Product           = S.Product,
                        Quantity          = S.Quantity,
                        Price             = S.Price
                    WHEN NOT MATCHED THEN INSERT
                        (OrderID, CustomerFirstName, CustomerLastName, Company, SalesDate,
                         EstimatedShipDate, ShippingID, ShippingLocation, Product, Quantity, Price)
                        VALUES (S.OrderID, S.CustomerFirstName, S.CustomerLastName, S.Company, S.SalesDate,
                                S.EstimatedShipDate, S.ShippingID, S.ShippingLocation, S.Product, S.Quantity, S.Price);
                    SET IDENTITY_INSERT dbo.Orders OFF;
                END
                ELSE IF @Operation = 'DEL'
                    DELETE FROM dbo.Orders WHERE OrderID = @OrderID;

                INSERT dbo.ces_ledger (partition_id, sequence_number, commit_lsn)
                VALUES (@PartitionId, @SequenceNumber, @CommitLsn);

                UPDATE dbo.ces_offsets
                   SET last_sequence_number = @SequenceNumber,
                       last_commit_lsn      = @CommitLsn,
                       updated_at           = sysdatetime()
                 WHERE partition_id = @PartitionId;
                IF @@ROWCOUNT = 0
                    INSERT dbo.ces_offsets (partition_id, last_sequence_number, last_commit_lsn)
                    VALUES (@PartitionId, @SequenceNumber, @CommitLsn);

                SET @applied = 1;
            END

            COMMIT;
            SELECT @applied;
            """;

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@PartitionId", partitionId);
        cmd.Parameters.AddWithValue("@SequenceNumber", sequenceNumber);
        cmd.Parameters.AddWithValue("@CommitLsn", ev.CommitLsn);
        cmd.Parameters.AddWithValue("@Operation", ev.Operation);
        cmd.Parameters.AddWithValue("@OrderID", ev.OrderId);
        cmd.Parameters.AddWithValue("@CustomerFirstName", ev.CustomerFirstName);
        cmd.Parameters.AddWithValue("@CustomerLastName", ev.CustomerLastName);
        cmd.Parameters.AddWithValue("@Company", ev.Company);
        cmd.Parameters.AddWithValue("@SalesDate", ev.SalesDate);
        cmd.Parameters.AddWithValue("@EstimatedShipDate", ev.EstimatedShipDate);
        cmd.Parameters.AddWithValue("@ShippingID", ev.ShippingId);
        cmd.Parameters.AddWithValue("@ShippingLocation", ev.ShippingLocation);
        cmd.Parameters.AddWithValue("@Product", ev.Product);
        cmd.Parameters.AddWithValue("@Quantity", ev.Quantity);
        cmd.Parameters.AddWithValue("@Price", ev.Price);

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result is true;
    }

    private sealed record ParsedOrderEvent(
        string Operation, string Table, int OrderId, string CommitLsn,
        object CustomerFirstName, object CustomerLastName, object Company,
        object SalesDate, object EstimatedShipDate, object ShippingId,
        object ShippingLocation, object Product, object Quantity, object Price);

    // Same envelope handling as KafkaConsumerService.ParseEvent, but extracts the
    // full row so it can be applied to the destination Orders table.
    private static ParsedOrderEvent? ParseEvent(string json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            var operation = root.TryGetProperty("operation", out var opProp)
                ? opProp.GetString() ?? "?"
                : "?";

            if (!root.TryGetProperty("data", out var dataProp)) return null;

            using var dataDoc = dataProp.ValueKind == JsonValueKind.String
                ? JsonDocument.Parse(dataProp.GetString()!)
                : JsonDocument.Parse(dataProp.GetRawText());
            var data = dataDoc.RootElement;

            var table = "";
            var orderId = 0;
            var commitLsn = "";

            if (data.TryGetProperty("eventsource", out var src))
            {
                table = src.TryGetProperty("tbl", out var t) ? t.GetString() ?? "" : "";

                if (src.TryGetProperty("pkkey", out var pk) && pk.ValueKind == JsonValueKind.Array)
                    foreach (var key in pk.EnumerateArray())
                    {
                        var value = key.TryGetProperty("value", out var v) ? v.GetString() ?? "" : "";
                        int.TryParse(value, out orderId);
                        break;
                    }

                if (src.TryGetProperty("transaction", out var txn))
                    commitLsn = FirstString(txn, "commitlsn", "lsn", "committime");
                if (commitLsn.Length == 0)
                    commitLsn = FirstString(src, "commitlsn", "lsn");
            }

            if (commitLsn.Length == 0) commitLsn = "n/a";
            if (commitLsn.Length > 64) commitLsn = commitLsn[..64];

            // Row image: present for INS/UPD; null for DEL
            JsonElement row = default;
            var hasRow = false;
            if (data.TryGetProperty("eventrow", out var rowProp) &&
                rowProp.TryGetProperty("current", out var cur) &&
                cur.ValueKind != JsonValueKind.Null)
            {
                using var rowDoc = cur.ValueKind == JsonValueKind.String
                    ? JsonDocument.Parse(cur.GetString()!)
                    : JsonDocument.Parse(cur.GetRawText());
                row = rowDoc.RootElement.Clone();
                hasRow = true;

                if (orderId == 0 && row.TryGetProperty("OrderID", out var idProp))
                    orderId = idProp.ValueKind == JsonValueKind.Number
                        ? idProp.GetInt32()
                        : int.TryParse(idProp.GetString(), out var id) ? id : 0;
            }

            return new ParsedOrderEvent(
                operation, table, orderId, commitLsn,
                GetString(row, hasRow, "CustomerFirstName"),
                GetString(row, hasRow, "CustomerLastName"),
                GetString(row, hasRow, "Company"),
                GetDate(row, hasRow, "SalesDate"),
                GetDate(row, hasRow, "EstimatedShipDate"),
                GetInt(row, hasRow, "ShippingID"),
                GetString(row, hasRow, "ShippingLocation"),
                GetString(row, hasRow, "Product"),
                GetInt(row, hasRow, "Quantity"),
                GetDecimal(row, hasRow, "Price"));
        }
        catch
        {
            return null;
        }
    }

    private static string FirstString(JsonElement el, params string[] names)
    {
        foreach (var name in names)
            if (el.TryGetProperty(name, out var p) && p.ValueKind == JsonValueKind.String &&
                p.GetString() is { Length: > 0 } s)
                return s;
        return "";
    }

    private static object GetString(JsonElement row, bool hasRow, string name) =>
        hasRow && row.TryGetProperty(name, out var p) && p.ValueKind == JsonValueKind.String
            ? p.GetString()!
            : DBNull.Value;

    private static object GetInt(JsonElement row, bool hasRow, string name)
    {
        if (!hasRow || !row.TryGetProperty(name, out var p)) return DBNull.Value;
        if (p.ValueKind == JsonValueKind.Number && p.TryGetInt32(out var n)) return n;
        if (p.ValueKind == JsonValueKind.String && int.TryParse(p.GetString(), out var s)) return s;
        return DBNull.Value;
    }

    private static object GetDecimal(JsonElement row, bool hasRow, string name)
    {
        if (!hasRow || !row.TryGetProperty(name, out var p)) return DBNull.Value;
        if (p.ValueKind == JsonValueKind.Number && p.TryGetDecimal(out var n)) return n;
        if (p.ValueKind == JsonValueKind.String &&
            decimal.TryParse(p.GetString(), System.Globalization.NumberStyles.Number,
                System.Globalization.CultureInfo.InvariantCulture, out var s)) return s;
        return DBNull.Value;
    }

    private static object GetDate(JsonElement row, bool hasRow, string name)
    {
        if (!hasRow || !row.TryGetProperty(name, out var p)) return DBNull.Value;
        if (p.ValueKind == JsonValueKind.String &&
            DateTime.TryParse(p.GetString(), System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.None, out var d)) return d;
        return DBNull.Value;
    }
}
