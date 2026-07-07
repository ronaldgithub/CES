using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Avalonia.Threading;
using CES.UI.ViewModels;
using Confluent.Kafka;
using Microsoft.Data.SqlClient;

namespace CES.UI.Services;

/// <summary>
/// Batching variant of the live consumers for the Batching (Live) tab: incoming
/// events pile up in the tab's queue, the user moves them into a batch, and
/// "Commit Batch" applies the whole batch in ONE SQL transaction — the offset
/// moves once per batch instead of once per event. A crash before the commit
/// persists nothing, and the Kafka replay after restart re-delivers the batch.
/// </summary>
public class BatchingLiveService(BatchingLiveTabViewModel viewModel)
{
    private const string BootstrapServers = "ces-poc-od.servicebus.windows.net:9093";
    private const string Topic = "orders";
    private const string ConsumerGroup = "batching";
    public const string DatabaseName = "CES_BatchingDemo";

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
                Dispatcher.UIThread.Post(() => viewModel.EnqueueIncoming(pending));
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

    public static async Task<(List<(PendingLiveEvent Event, bool Applied)> Results, int OrderRowCount)>
        CommitBatchAsync(IReadOnlyList<PendingLiveEvent> batch, CancellationToken cancellationToken)
    {
        await using var conn = new SqlConnection(DestinationDatabase.ConnectionString(DatabaseName));
        await conn.OpenAsync(cancellationToken);

        var results = await OrderEventApplier.ApplyBatchAsync(conn, batch, cancellationToken);
        var rows = await DestinationDatabase.CountOrdersAsync(conn, cancellationToken);

        return (results, rows);
    }

    public static Task ResetDatabaseAsync(CancellationToken cancellationToken) =>
        DestinationDatabase.ResetAsync(DatabaseName, cancellationToken);
}
