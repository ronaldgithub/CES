using System;
using System.Collections.ObjectModel;
using System.Linq;
using CES.UI.Models;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;

namespace CES.UI.ViewModels;

public partial class MultiTableTabViewModel : ObservableObject
{
    private static readonly SimulatedEvent[] CannedEvents =
    [
        new(0, 200, "LSN_700", "INS", "dbo", "Orders", "OrderID=1", "OrderID=1, Status=Pending"),
        new(0, 201, "LSN_701", "INS", "dbo", "OrderLines", "OrderLineID=1", "OrderLineID=1, OrderID=1, ProductID=9, Qty=2"),
        new(0, 202, "LSN_702", "UPD", "dbo", "Orders", "OrderID=1", "OrderID=1, Status=Shipped"),
        new(0, 203, "LSN_703", "INS", "dbo", "OrderLines", "OrderLineID=2", "OrderLineID=2, OrderID=1, ProductID=4, Qty=1"),
        new(0, 204, "LSN_704", "DEL", "dbo", "OrderLines", "OrderLineID=1", "OrderLineID=1 removed")
    ];

    private int _cursor;

    public ObservableCollection<LedgerEntry> Ledger { get; } = [];

    public ObservableCollection<string> OrdersRows { get; } = [];

    public ObservableCollection<string> OrderLinesRows { get; } = [];

    public ObservableCollection<string> Log { get; } = [];

    [ObservableProperty]
    private OffsetEntry? _currentOffset;

    partial void OnCurrentOffsetChanged(OffsetEntry? value) => OnPropertyChanged(nameof(OffsetSummary));

    public string OffsetSummary => CurrentOffset is null
        ? "Partition: — · Last seq: — · Last LSN: —"
        : $"Partition: {CurrentOffset.PartitionId} · Last seq: {CurrentOffset.LastSequenceNumber} · Last LSN: {CurrentOffset.LastCommitLsn}";

    public string NextEventPreview => _cursor < CannedEvents.Length
        ? $"Next: seq {CannedEvents[_cursor].SequenceNumber} {CannedEvents[_cursor].Operation} on {CannedEvents[_cursor].Schema}.{CannedEvents[_cursor].Table}"
        : "All events processed.";

    private bool CanProcessNextEvent => _cursor < CannedEvents.Length;

    [RelayCommand(CanExecute = nameof(CanProcessNextEvent))]
    private void ProcessNextEvent()
    {
        var ev = CannedEvents[_cursor];
        _cursor++;

        var isDuplicate = Ledger.Any(l => l.PartitionId == ev.PartitionId && l.SequenceNumber == ev.SequenceNumber);
        if (isDuplicate)
        {
            Log.Insert(0, $"seq {ev.SequenceNumber} — DUPLICATE, already in shared ledger. Skipped.");
        }
        else
        {
            var targetRows = ev.Table == "Orders" ? OrdersRows : OrderLinesRows;
            ApplyRow(targetRows, ev);
            Ledger.Add(new LedgerEntry(ev.PartitionId, ev.SequenceNumber, ev.CommitLsn, DateTime.Now));
            CurrentOffset = new OffsetEntry(ev.PartitionId, ev.SequenceNumber, ev.CommitLsn, DateTime.Now);
            Log.Insert(0, $"seq {ev.SequenceNumber} — routed to {ev.Schema}.{ev.Table}. Shared ledger + offset updated.");
        }

        OnPropertyChanged(nameof(NextEventPreview));
        ProcessNextEventCommand.NotifyCanExecuteChanged();
    }

    private static void ApplyRow(ObservableCollection<string> rows, SimulatedEvent ev)
    {
        var existing = rows.FirstOrDefault(r => r.StartsWith(ev.PrimaryKey, StringComparison.Ordinal));

        if (ev.Operation == "DEL")
        {
            if (existing is not null) rows.Remove(existing);
            return;
        }

        if (existing is not null)
            rows[rows.IndexOf(existing)] = ev.Payload;
        else
            rows.Add(ev.Payload);
    }

    [RelayCommand]
    private void Reset()
    {
        _cursor = 0;
        Ledger.Clear();
        OrdersRows.Clear();
        OrderLinesRows.Clear();
        Log.Clear();
        CurrentOffset = null;
        OnPropertyChanged(nameof(NextEventPreview));
        ProcessNextEventCommand.NotifyCanExecuteChanged();
    }
}
