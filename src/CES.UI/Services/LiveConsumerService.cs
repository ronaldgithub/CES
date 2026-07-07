using System;
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
/// pattern from docs/ces_idempotent.sql (see OrderEventApplier).
///
/// Kafka offsets are deliberately never committed (EnableAutoCommit = false), so
/// every Start replays the stream from the beginning — the ces_ledger table is
/// what makes that replay safe.
/// </summary>
public class LiveConsumerService(LiveConsumerState state)
{
    private const string BootstrapServers = "ces-poc-od.servicebus.windows.net:9093";
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

                var ev = OrderEventApplier.Parse(result.Message.Value);
                if (ev is null || ev.Table != "Orders") continue;

                var partitionId = result.Partition.Value;
                var sequenceNumber = result.Offset.Value;

                var applied = await OrderEventApplier.ApplyAsync(conn, ev, partitionId, sequenceNumber, cancellationToken);

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
}
