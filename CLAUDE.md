# CLAUDE.md

## Project

SQL Server 2025 CES Monitor — Avalonia dark-mode desktop app that consumes Change Event Streaming events from Azure Event Hubs and displays them live.

## Stack

- **UI**: Avalonia 11 (dark FluentTheme, Win32 renderer, `net8.0`)
- **MVVM**: CommunityToolkit.Mvvm (`[ObservableProperty]` source generators)
- **Kafka consumer**: Confluent.Kafka 2.8 connecting to Azure Event Hubs Kafka endpoint (port 9093, SASL/SSL)
- **Event parsing**: System.Text.Json — no CloudNative.CloudEvents package needed; CES emits raw JSON

## Key files

| File | Purpose |
|---|---|
| `src/CES.UI/Services/KafkaConsumerService.cs` | Background consumer loop; posts to UI via `Dispatcher.UIThread.Post()` |
| `src/CES.UI/ViewModels/MainWindowViewModel.cs` | `ObservableCollection<ChangeEvent>` + status string |
| `src/CES.UI/Views/MainWindow.axaml` | Dark event feed UI — colour-coded INS/UPD/DEL badges |
| `src/CES.UI/Converters/OperationColorConverter.cs` | Maps operation string to badge background colour |
| `scripts/enableces_kafka.sql` | CES setup: credential + stream group → Event Hubs |
| `scripts/orders_ddl.sql` | Creates `ContosoOrders` DB + `Orders` table |

## CES JSON payload structure

Events arrive with this shape (from Bob Ward's examples):

```json
{
  "operation": "INS",
  "data": {
    "eventsource": {
      "db": "ContosoOrders", "schema": "dbo", "tbl": "Orders",
      "pkkey": [{ "columnname": "OrderID", "value": "1" }],
      "transaction": { "committime": "..." }
    },
    "eventrow": {
      "current": "{\"OrderID\":1,\"CustomerFirstName\":\"Art\",...}",
      "old": null
    }
  }
}
```

Both `data` and `eventrow.current` can be either JSON strings or JSON objects — the parser handles both.

## Build & run

```powershell
# Set secret (never commit this)
. .\set-env.local.ps1

dotnet build src\CES.UI
dotnet run --project src\CES.UI
```

## Secrets

- `CES_CONNECTION_STRING` env var — full Event Hubs connection string
- `set-env.local.ps1` — local file with real values, gitignored
- SQL script uses placeholder `<your-sas-primary-key-here>`

## Azure Event Hubs config

- Namespace: `ces-poc.servicebus.windows.net`
- Event Hub: `orders`
- Consumer group: `$Default`
- Protocol: Kafka (port 9093, SASL/SSL, username `$ConnectionString`)

## SQL Server CES config

```sql
EXEC sys.sp_create_event_stream_group
    @stream_group_name      = N'OrdersCESGroupKafka',
    @destination_type       = N'AzureEventHubsAmqp',
    @destination_location   = N'ces-poc.servicebus.windows.net/orders',
    @destination_credential = eventhubscred;
```

Note: `destination_type` is always `AzureEventHubsAmqp` from SQL Server's side — the Kafka protocol is used only on the consumer (app) side.

## User profile

Ronald de Groot — experienced developer, building this as a POC and blog post about SQL Server 2025 CES. Prefers step-by-step guidance and simple/direct code without over-engineering.
