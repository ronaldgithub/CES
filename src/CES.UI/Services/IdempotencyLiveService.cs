using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Avalonia.Threading;
using CES.UI.Models;
using CES.UI.ViewModels;
using Confluent.Kafka;
using Microsoft.Data.SqlClient;

namespace CES.UI.Services;

/// <summary>
/// Single-step variant of LiveConsumerService for the Idempotency & Offsets (Live)
/// tab: the consume loop only buffers incoming events; nothing touches the
/// destination database until the user clicks "Process Next Event". On connect the
/// existing ces_ledger/ces_offsets rows are loaded from the database, so an app
/// restart shows the ledger surviving — which is the point of the demo.
/// </summary>
public class IdempotencyLiveService(IdempotencyLiveTabViewModel viewModel)
{
    private const string BootstrapServers = "ces-poc-od.servicebus.windows.net:9093";
    private const string Topic = "orders";
    private const string ConsumerGroup = "idempotency";
    private const string DatabaseName = "CES_IdempotencyDemo";

    private const string SqlConnectionString =
        $"Server=localhost;Database={DatabaseName};Integrated Security=True;TrustServerCertificate=True";

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
            var (ledger, offset) = await LoadStateAsync(cancellationToken);
            Dispatcher.UIThread.Post(() => viewModel.InitializeFromDb(ledger, offset));

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

                var ev = OrderEventApplier.Parse(result.Message.Value);
                if (ev is null || ev.Table != "Orders") continue;

                var pending = new PendingLiveEvent(ev, result.Partition.Value, result.Offset.Value);
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

    // A fresh connection per operation: apply/reset are user-paced single clicks,
    // and this avoids sharing one SqlConnection between the UI and consume loop.
    public static async Task<(bool Applied, int OrderRowCount)> ApplyAsync(
        PendingLiveEvent pending, CancellationToken cancellationToken)
    {
        await using var conn = new SqlConnection(SqlConnectionString);
        await conn.OpenAsync(cancellationToken);

        var applied = await OrderEventApplier.ApplyAsync(
            conn, pending.Event, pending.PartitionId, pending.SequenceNumber, cancellationToken);

        await using var count = new SqlCommand("SELECT COUNT(*) FROM dbo.Orders", conn);
        var rows = (int)(await count.ExecuteScalarAsync(cancellationToken))!;

        return (applied, rows);
    }

    public static async Task ResetDatabaseAsync(CancellationToken cancellationToken)
    {
        await using var conn = new SqlConnection(SqlConnectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand(
            "TRUNCATE TABLE dbo.ces_ledger; TRUNCATE TABLE dbo.ces_offsets; DELETE FROM dbo.Orders;", conn);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<(List<LedgerEntry> Ledger, OffsetEntry? Offset)> LoadStateAsync(
        CancellationToken cancellationToken)
    {
        await using var conn = new SqlConnection(SqlConnectionString);
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
}

public sealed record PendingLiveEvent(ParsedOrderEvent Event, int PartitionId, long SequenceNumber);
