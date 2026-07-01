using System;
using System.Collections.ObjectModel;
using System.Linq;
using CES.UI.Models;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;

namespace CES.UI.ViewModels;

public partial class IdempotencyTabViewModel : ObservableObject
{
    private static readonly SimulatedEvent[] CannedEvents =
    [
        new(0, 100, "LSN_500", "INS", "dbo", "Orders", "OrderID=1", "Status='Pending'"),
        new(0, 101, "LSN_501", "UPD", "dbo", "Orders", "OrderID=1", "Status='Shipped'"),
        new(0, 101, "LSN_501", "UPD", "dbo", "Orders", "OrderID=1", "Status='Shipped' (duplicate replay)")
    ];

    private int _cursor;

    public ObservableCollection<LedgerEntry> Ledger { get; } = [];

    public ObservableCollection<string> Log { get; } = [];

    [ObservableProperty]
    private OffsetEntry? _currentOffset;

    partial void OnCurrentOffsetChanged(OffsetEntry? value) => OnPropertyChanged(nameof(OffsetSummary));

    public string OffsetSummary => CurrentOffset is null
        ? "Partition: — · Last seq: — · Last LSN: —"
        : $"Partition: {CurrentOffset.PartitionId} · Last seq: {CurrentOffset.LastSequenceNumber} · Last LSN: {CurrentOffset.LastCommitLsn}";

    [ObservableProperty]
    private string _targetTableSummary = "(no rows applied yet)";

    public string NextEventPreview => _cursor < CannedEvents.Length
        ? $"Next: seq {CannedEvents[_cursor].SequenceNumber} {CannedEvents[_cursor].Operation} {CannedEvents[_cursor].PrimaryKey}" +
          (_cursor == 2 ? " — duplicate replay" : "")
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
            Log.Insert(0, $"seq {ev.SequenceNumber} — DUPLICATE, already in ledger. Skipped: no DML, no ledger insert, no offset update.");
        }
        else
        {
            TargetTableSummary = $"{ev.PrimaryKey} -> {ev.Payload}";
            Ledger.Add(new LedgerEntry(ev.PartitionId, ev.SequenceNumber, ev.CommitLsn, DateTime.Now));
            CurrentOffset = new OffsetEntry(ev.PartitionId, ev.SequenceNumber, ev.CommitLsn, DateTime.Now);
            Log.Insert(0, $"seq {ev.SequenceNumber} — applied. Ledger + offset updated.");
        }

        OnPropertyChanged(nameof(NextEventPreview));
        ProcessNextEventCommand.NotifyCanExecuteChanged();
    }

    [RelayCommand]
    private void Reset()
    {
        _cursor = 0;
        Ledger.Clear();
        Log.Clear();
        CurrentOffset = null;
        TargetTableSummary = "(no rows applied yet)";
        OnPropertyChanged(nameof(NextEventPreview));
        ProcessNextEventCommand.NotifyCanExecuteChanged();
    }
}
