using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Avalonia.Threading;
using CES.UI.Models;
using CES.UI.ViewModels;
using Confluent.Kafka;

namespace CES.UI.Services;

public class KafkaConsumerService(MainWindowViewModel viewModel)
{
    private const string BootstrapServers = "ces-poc.servicebus.windows.net:9093";
    private const string Topic = "orders";   // Event Hub name
    private const string GroupId = "$Default";  // Event Hubs default consumer group

    // Set this environment variable before running:  $env:CES_CONNECTION_STRING = "Endpoint=sb://..."
    private static readonly string ConnectionString =
        Environment.GetEnvironmentVariable("CES_CONNECTION_STRING")
        ?? throw new InvalidOperationException(
            "Set the CES_CONNECTION_STRING environment variable to your Event Hubs connection string.");

    public void Start(CancellationToken cancellationToken) =>
        Task.Run(() => ConsumeLoop(cancellationToken), cancellationToken);

    private async Task ConsumeLoop(CancellationToken cancellationToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = "$ConnectionString",
            SaslPassword = ConnectionString
        };

        try
        {
            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe(Topic);

            await Dispatcher.UIThread.InvokeAsync(() =>
                viewModel.Status = $"Connected  ·  {BootstrapServers}  ·  topic: {Topic}");

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(cancellationToken);
                    if (result?.Message?.Value is null) continue;

                    var ev = ParseEvent(result.Message.Value);
                    if (ev is not null)
                    {
                        Dispatcher.UIThread.Post(() =>
                        {
                            viewModel.Events.Insert(0, ev);
                            viewModel.EventCount++;
                        });
                    }
                }
                catch (ConsumeException ex) when (!cancellationToken.IsCancellationRequested)
                {
                    await Dispatcher.UIThread.InvokeAsync(() =>
                        viewModel.Status = $"Consume error: {ex.Error.Reason}");
                }
            }

            consumer.Close();
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            await Dispatcher.UIThread.InvokeAsync(() =>
                viewModel.Status = $"Connection failed: {ex.Message}");
        }
    }

    // CES event body structure (from Bob Ward's examples):
    // {
    //   "operation": "INS",
    //   "data": {                          ← may be a JSON string or JSON object
    //     "eventsource": {
    //       "db": "ContosoOrders",
    //       "schema": "dbo",
    //       "tbl": "Orders",
    //       "pkkey": [{ "columnname": "OrderID", "value": "1" }],
    //       "transaction": { "committime": "..." }
    //     },
    //     "eventrow": {
    //       "current": "{...row JSON...}", ← JSON string of column values
    //       "old": null
    //     }
    //   }
    // }
    private static ChangeEvent? ParseEvent(string json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            var operation = root.TryGetProperty("operation", out var opProp)
                ? opProp.GetString() ?? "?"
                : "?";

            if (!root.TryGetProperty("data", out var dataProp)) return null;

            // data can be a JSON string or a JSON object
            using var dataDoc = dataProp.ValueKind == JsonValueKind.String
                ? JsonDocument.Parse(dataProp.GetString()!)
                : JsonDocument.Parse(dataProp.GetRawText());

            var data = dataDoc.RootElement;

            var db = "";
            var schema = "";
            var table = "";
            var pkValue = "";

            if (data.TryGetProperty("eventsource", out var src))
            {
                db     = src.TryGetProperty("db",     out var p) ? p.GetString() ?? "" : "";
                schema = src.TryGetProperty("schema", out p)     ? p.GetString() ?? "" : "";
                table  = src.TryGetProperty("tbl",    out p)     ? p.GetString() ?? "" : "";

                if (src.TryGetProperty("pkkey", out var pk) && pk.ValueKind == JsonValueKind.Array)
                    foreach (var key in pk.EnumerateArray())
                    {
                        pkValue = key.TryGetProperty("value", out var v) ? v.GetString() ?? "" : "";
                        break;
                    }
            }

            var customerName = "";
            if (data.TryGetProperty("eventrow", out var rowProp) &&
                rowProp.TryGetProperty("current", out var cur) &&
                cur.ValueKind != JsonValueKind.Null)
            {
                // current row is also a JSON string
                using var rowDoc = cur.ValueKind == JsonValueKind.String
                    ? JsonDocument.Parse(cur.GetString()!)
                    : JsonDocument.Parse(cur.GetRawText());
                customerName = ExtractCustomerName(rowDoc.RootElement);
            }

            return new ChangeEvent(
                operation,
                db,
                schema,
                table,
                string.IsNullOrEmpty(pkValue) ? "" : $"ID: {pkValue}",
                customerName,
                DateTime.Now);
        }
        catch
        {
            return null;
        }
    }

    private static string ExtractCustomerName(JsonElement row)
    {
        var first = row.TryGetProperty("CustomerFirstName", out var f) ? f.GetString() ?? "" : "";
        var last  = row.TryGetProperty("CustomerLastName",  out var l) ? l.GetString() ?? "" : "";
        return $"{first} {last}".Trim();
    }
}
