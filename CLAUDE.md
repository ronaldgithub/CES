# CLAUDE.md

## Project

SQL Server 2025 CES Monitor — Avalonia dark-mode desktop app that consumes Change Event Streaming events from Azure Event Hubs and displays them live. Also includes 5 in-memory scenario-simulation tabs (no Event Hub/SQL Server needed) that demonstrate CES consumer design patterns from `docs/ces_idempotent.sql`, plus four live tabs where real consumers apply the stream to local destination databases: "Idempotency (Live)" (single-step), "Two Consumers (Live)", "Batching (Live)" (one transaction per batch) and "Multi-Table (Live)" (Orders + OrderLines routing behind one shared ledger).

## Stack

- **UI**: Avalonia 11 (dark FluentTheme, Win32 renderer, `net8.0`)
- **MVVM**: CommunityToolkit.Mvvm (`[ObservableProperty]` source generators)
- **Kafka consumer**: Confluent.Kafka 2.8 connecting to Azure Event Hubs Kafka endpoint (port 9093, SASL/SSL)
- **Event parsing**: System.Text.Json — no CloudNative.CloudEvents package needed; CES emits raw JSON
- **SQL apply**: Microsoft.Data.SqlClient 7 — live consumers write to local SQL Server (`Server=localhost;Integrated Security=True;TrustServerCertificate=True`)

## Key files

| File | Purpose |
| --- | --- |
| `src/CES.UI/Services/KafkaConsumerService.cs` | Background consumer loop (Live Feed, display only); posts to UI via `Dispatcher.UIThread.Post()` |
| `src/CES.UI/Services/LiveConsumerService.cs` | Real consumer for the live tab: Kafka → single-transaction idempotent apply (ledger check → MERGE/DELETE with `IDENTITY_INSERT` → ledger insert → offset upsert) |
| `src/CES.UI/Services/OrderEventApplier.cs` | Shared parse + idempotent-apply logic (single-event and batch) used by all live consumer services |
| `src/CES.UI/Services/DestinationDatabase.cs` | Shared load-state/reset/count helpers for the local destination DBs |
| `src/CES.UI/Services/IdempotencyLiveService.cs` | Single-step consumer for the Idempotency (Live) tab: buffers events, applies one per click to `CES_IdempotencyDemo` |
| `src/CES.UI/Services/BatchingLiveService.cs` | Batch-commit consumer for the Batching (Live) tab: buffers events, applies a batch per commit to `CES_Batching` |
| `src/CES.UI/Services/MultiTableLiveService.cs` | Table-routing consumer for the Multi-Table (Live) tab: routes Orders/OrderLines events to `CES_MultiTable` behind one shared ledger |
| `src/CES.UI/ViewModels/MainWindowViewModel.cs` | Shell VM — `ObservableCollection<ChangeEvent>` + status string, plus one property per scenario tab VM |
| `src/CES.UI/Views/MainWindow.axaml` | `TabControl` shell — Live Feed + 5 scenario tabs |
| `src/CES.UI/Views/LiveFeedView.axaml` | Dark event feed UI — colour-coded INS/UPD/DEL badges (moved out of MainWindow) |
| `src/CES.UI/Converters/OperationColorConverter.cs` | Maps operation string to badge background colour |
| `scripts/ces_demo.sql` | The whole SQL side in 7 parts: source DB, enable CES + credential, stream group, verify, destination DBs for the live tab, test events, diagnostics/recovery |
| `scripts/neworder.sql` | Quick single INSERT for repeated demo runs |
| `docs/ces_idempotent.sql` | Design notes for the 5 consumer scenarios (source of truth for the simulation tabs) — not a runnable script |

### Scenario simulation tabs

Each is a standalone in-memory simulation (no Kafka/SQL Server) with its own ViewModel + `UserControl` view. All follow the same pattern: canned `SimulatedEvent`s, an `ObservableCollection<LedgerEntry>` ledger, an `OffsetEntry?` offset, and `[RelayCommand]` buttons that replicate the idempotency-check-then-apply flow directly (no shared "ledger service" — kept inline per tab for readability).

| Tab | ViewModel | Demonstrates |
| --- | --- | --- |
| Idempotency & Offsets | `ViewModels/IdempotencyTabViewModel.cs` | Duplicate event replay is detected via the ledger and skipped |
| Two Consumers | `ViewModels/TwoConsumersTabViewModel.cs` | Independent consumers (`ConsumerState`) keep separate ledgers/offsets |
| Parallel Partitions | `ViewModels/ParallelPartitionsTabViewModel.cs` | 4 independent partition workers (`PartitionWorkerState`) |
| Multi-Table Routing | `ViewModels/MultiTableTabViewModel.cs` | One shared ledger/offset routes events to `Orders`/`OrderLines` by table |
| Batching | `ViewModels/BatchingTabViewModel.cs` | Buffered batch commit updates the offset once; crash simulation proves replay-safety |

`Models/SimulatedEvent.cs` and `Models/SimulationModels.cs` (`LedgerEntry`, `OffsetEntry`) are the shared plain record types used across all 5 tabs.

### Idempotency (Live) tab

`ViewModels/IdempotencyLiveTabViewModel.cs` (+ `Views/IdempotencyLiveView.axaml`) is the live twin of the Idempotency & Offsets simulation: consumer group `idempotency` → `CES_IdempotencyDemo`. `IdempotencyLiveService` only buffers incoming events into a queue; **Process Next Event** applies exactly one via `OrderEventApplier`. On Start the existing `ces_ledger`/`ces_offsets` rows are loaded from the DB (ledger survives restarts); **Reset DB** truncates them.

### Batching (Live) tab

`ViewModels/BatchingLiveTabViewModel.cs` (+ `Views/BatchingLiveView.axaml`) is the live twin of the Batching simulation: consumer group `batching` → `CES_Batching`. Incoming events queue up; **Add Next Event to Batch** buffers up to 5 (no SQL); **Commit Batch** applies them via `OrderEventApplier.ApplyBatchAsync` in one `SqlTransaction` — ledger check/insert per event, offset upserted once per partition. **Simulate Crash Mid-Batch** discards the uncommitted batch (nothing was persisted); Stop + Start replays.

### Multi-Table (Live) tab

`ViewModels/MultiTableLiveTabViewModel.cs` (+ `Views/MultiTableLiveView.axaml`) is the live twin of the Multi-Table Routing simulation: consumer group `multitable` → `CES_MultiTable`, which holds **both** `Orders` and `OrderLines` (both tables are in the CES stream group). `OrderEventApplier.ParseEnvelope` gives the table-independent envelope; **Process Next Event** maps it via `ToOrderEvent`/`ToOrderLineEvent` and applies to the matching table — one shared `ces_ledger`/`ces_offsets` for both. `OrderLines.LineTotal` is computed and never written; the destination has no FK because cross-table event order isn't guaranteed across partitions.

### Two Consumers (Live) tab

`ViewModels/TwoConsumersLiveTabViewModel.cs` (+ `Views/TwoConsumersLiveView.axaml`, `Models/LiveApplyEntry.cs`) hosts two `LiveConsumerState` panels, each starting a `LiveConsumerService`: consumer group `consumer1` → `CES_Destination1`, `consumer2` → `CES_Destination2`. Kafka offsets are deliberately never committed (`EnableAutoCommit = false`), so every Start replays the full stream and the `ces_ledger` table proves replay-safety — exactly-once lives in the destination DB, not the transport.

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

- Namespace: `ces-poc-od.servicebus.windows.net`
- Event Hub: `orders`
- Consumer groups: `$Default` (Live Feed), `consumer1`/`consumer2` (Two Consumers Live), `idempotency` (Idempotency Live), `batching` (Batching Live), `multitable` (Multi-Table Live)
- Protocol: Kafka (port 9093, SASL/SSL, username `$ConnectionString`)

## SQL Server CES config

```sql
EXEC sys.sp_create_event_stream_group
    @stream_group_name      = N'OrdersCESGroupKafka',
    @destination_type       = N'AzureEventHubsAmqp',
    @destination_location   = N'ces-poc-od.servicebus.windows.net/orders',
    @destination_credential = eventhubscred;
```

Note: `destination_type` is always `AzureEventHubsAmqp` from SQL Server's side — the Kafka protocol is used only on the consumer (app) side.

The stream group contains two tables: `dbo.Orders` and `dbo.OrderLines` (added for the Multi-Table Live tab).

## User profile

Ronald de Groot — experienced developer, building this as a POC and blog post about SQL Server 2025 CES. Prefers step-by-step guidance and simple/direct code without over-engineering.
