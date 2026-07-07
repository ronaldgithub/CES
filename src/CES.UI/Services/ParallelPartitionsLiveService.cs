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
/// Partition-parallel variant of the live consumers for the Parallel Partitions
/// (Live) tab: one subscription (consumer group "partitions") feeds four
/// per-partition queues — the orders hub has 4 partitions. Each "Process Next
/// Event (all partitions)" tick applies one event per partition concurrently to
/// CES_Partitions; ces_ledger/ces_offsets are keyed by partition_id, so every
/// worker tracks its own position.
/// </summary>
public class ParallelPartitionsLiveService(ParallelPartitionsLiveTabViewModel viewModel)
{
    private const string BootstrapServers = "ces-poc-od.servicebus.windows.net:9093";
    private const string Topic = "orders";
    private const string ConsumerGroup = "partitions";
    public const string DatabaseName = "CES_Partitions";

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
            var (ledger, offsets) = await LoadStateAsync(cancellationToken);
            Dispatcher.UIThread.Post(() => viewModel.InitializeFromDb(ledger, offsets));

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

    // One connection per apply — the whole point of this tab is that the four
    // partition workers run these concurrently.
    public static async Task<bool> ApplyAsync(PendingLiveEvent pending, CancellationToken cancellationToken)
    {
        await using var conn = new SqlConnection(DestinationDatabase.ConnectionString(DatabaseName));
        await conn.OpenAsync(cancellationToken);

        return await OrderEventApplier.ApplyAsync(
            conn, pending.Event, pending.PartitionId, pending.SequenceNumber, cancellationToken);
    }

    public static Task ResetDatabaseAsync(CancellationToken cancellationToken) =>
        DestinationDatabase.ResetAsync(DatabaseName, cancellationToken);

    private static async Task<(List<LedgerEntry> Ledger, List<OffsetEntry> Offsets)> LoadStateAsync(
        CancellationToken cancellationToken)
    {
        var (ledger, _) = await DestinationDatabase.LoadStateAsync(DatabaseName, cancellationToken);

        var offsets = new List<OffsetEntry>();
        await using var conn = new SqlConnection(DestinationDatabase.ConnectionString(DatabaseName));
        await conn.OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand(
            "SELECT partition_id, last_sequence_number, last_commit_lsn, updated_at FROM dbo.ces_offsets", conn);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            offsets.Add(new OffsetEntry(
                reader.GetInt32(0), reader.GetInt64(1), reader.GetString(2), reader.GetDateTime(3)));

        return (ledger, offsets);
    }
}
