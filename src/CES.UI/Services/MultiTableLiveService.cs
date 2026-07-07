using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Avalonia.Threading;
using CES.UI.ViewModels;
using Confluent.Kafka;
using Microsoft.Data.SqlClient;

namespace CES.UI.Services;

/// <summary>A pending event that has not been mapped to a table yet.</summary>
public sealed record PendingEnvelopeEvent(ParsedEnvelope Envelope, int PartitionId, long SequenceNumber)
{
    public string Display => $"seq {SequenceNumber} {Envelope.Operation} on dbo.{Envelope.Table}";
}

/// <summary>
/// Multi-table variant of the live consumers for the Multi-Table (Live) tab:
/// Orders AND OrderLines events arrive on the same stream (both tables are in the
/// stream group), buffer in a queue, and each "Process Next Event" click routes
/// one of them to the matching table in CES_MultiTable — with ONE shared
/// ces_ledger/ces_offsets guarding both tables.
/// </summary>
public class MultiTableLiveService(MultiTableLiveTabViewModel viewModel)
{
    private const string BootstrapServers = "ces-poc-od.servicebus.windows.net:9093";
    private const string Topic = "orders";
    private const string ConsumerGroup = "multitable";
    public const string DatabaseName = "CES_MultiTable";

    private static readonly string EventHubsConnectionString =
        Environment.GetEnvironmentVariable("CES_CONNECTION_STRING")
        ?? throw new InvalidOperationException(
            "Set the CES_CONNECTION_STRING environment variable to your Event Hubs connection string.");

    public void Start(CancellationToken cancellationToken) =>
        Task.Run(() => ConsumeLoop(cancellationToken), cancellationToken);

    private async Task ConsumeLoop(CancellationToken cancellationToken)
    {
        try
        {
            // Show what is already in the destination database before consuming
            var (ledger, offset) = await DestinationDatabase.LoadStateAsync(DatabaseName, cancellationToken);
            var (orders, orderLines) = await LoadTablesAsync(cancellationToken);
            Dispatcher.UIThread.Post(() => viewModel.InitializeFromDb(ledger.Count, offset, orders, orderLines));

            var config = new ConsumerConfig
            {
                BootstrapServers = BootstrapServers,
                GroupId = ConsumerGroup,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,   // always replay from the start — the ledger dedups
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "$ConnectionString",
                SaslPassword = EventHubsConnectionString
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe(Topic);

            Dispatcher.UIThread.Post(() =>
                viewModel.Status = $"Connected · group {ConsumerGroup} → {DatabaseName}");

            while (!cancellationToken.IsCancellationRequested)
            {
                ConsumeResult<Ignore, string>? result;
                try
                {
                    result = consumer.Consume(cancellationToken);
                }
                catch (ConsumeException ex) when (!cancellationToken.IsCancellationRequested)
                {
                    Dispatcher.UIThread.Post(() => viewModel.Status = $"Consume error: {ex.Error.Reason}");
                    continue;
                }

                if (result?.Message?.Value is null) continue;

                var env = OrderEventApplier.ParseEnvelope(result.Message.Value);
                if (env is null || env.Table is not ("Orders" or "OrderLines")) continue;

                var pending = new PendingEnvelopeEvent(env, result.Partition.Value, result.Offset.Value);
                Dispatcher.UIThread.Post(() => viewModel.EnqueuePending(pending));
            }

            consumer.Close();
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Dispatcher.UIThread.Post(() => viewModel.MarkStopped($"Failed: {ex.Message}"));
            return;
        }

        Dispatcher.UIThread.Post(() => viewModel.MarkStopped("Stopped"));
    }

    public static async Task<bool> ApplyAsync(PendingEnvelopeEvent pending, CancellationToken cancellationToken)
    {
        await using var conn = new SqlConnection(DestinationDatabase.ConnectionString(DatabaseName));
        await conn.OpenAsync(cancellationToken);

        return pending.Envelope.Table == "Orders"
            ? await OrderEventApplier.ApplyAsync(
                conn, OrderEventApplier.ToOrderEvent(pending.Envelope),
                pending.PartitionId, pending.SequenceNumber, cancellationToken)
            : await OrderEventApplier.ApplyOrderLineAsync(
                conn, OrderEventApplier.ToOrderLineEvent(pending.Envelope),
                pending.PartitionId, pending.SequenceNumber, cancellationToken);
    }

    public static async Task<(List<string> Orders, List<string> OrderLines)> LoadTablesAsync(
        CancellationToken cancellationToken)
    {
        await using var conn = new SqlConnection(DestinationDatabase.ConnectionString(DatabaseName));
        await conn.OpenAsync(cancellationToken);

        var orders = new List<string>();
        await using (var cmd = new SqlCommand(
            "SELECT TOP 50 OrderID, Product, Quantity, Price FROM dbo.Orders ORDER BY OrderID DESC", conn))
        await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
                orders.Add($"OrderID={reader.GetInt32(0)} · {(reader.IsDBNull(1) ? "?" : reader.GetString(1))}" +
                           $" · Qty {(reader.IsDBNull(2) ? 0 : reader.GetInt32(2))}" +
                           $" · {(reader.IsDBNull(3) ? 0m : reader.GetDecimal(3)):0.00}");
        }

        var orderLines = new List<string>();
        await using (var cmd = new SqlCommand(
            "SELECT TOP 50 OrderLineID, OrderID, ProductID, Quantity, UnitPrice, LineTotal FROM dbo.OrderLines ORDER BY OrderLineID DESC", conn))
        await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
                orderLines.Add($"OrderLineID={reader.GetInt32(0)} · OrderID={reader.GetInt32(1)}" +
                               $" · ProductID={reader.GetInt32(2)} · {reader.GetInt32(3)} × {reader.GetDecimal(4):0.00}" +
                               $" = {(reader.IsDBNull(5) ? 0m : reader.GetDecimal(5)):0.00}");
        }

        return (orders, orderLines);
    }

    public static async Task ResetDatabaseAsync(CancellationToken cancellationToken)
    {
        await using var conn = new SqlConnection(DestinationDatabase.ConnectionString(DatabaseName));
        await conn.OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand(
            "TRUNCATE TABLE dbo.ces_ledger; TRUNCATE TABLE dbo.ces_offsets; DELETE FROM dbo.OrderLines; DELETE FROM dbo.Orders;", conn);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }
}
