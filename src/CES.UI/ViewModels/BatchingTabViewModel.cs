using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using CES.UI.Models;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;

namespace CES.UI.ViewModels;

public partial class BatchingTabViewModel : ObservableObject
{
    private const int BatchSize = 5;

    private readonly List<SimulatedEvent> _seedEvents = BuildSeedEvents();

    public ObservableCollection<SimulatedEvent> IncomingQueue { get; } = [];

    public ObservableCollection<SimulatedEvent> CurrentBatchPreview { get; } = [];

    public ObservableCollection<LedgerEntry> Ledger { get; } = [];

    public ObservableCollection<string> Log { get; } = [];

    [ObservableProperty]
    private OffsetEntry? _currentOffset;

    partial void OnCurrentOffsetChanged(OffsetEntry? value) => OnPropertyChanged(nameof(OffsetSummary));

    public string OffsetSummary => CurrentOffset is null
        ? "Last seq: — · Last LSN: —"
        : $"Last seq: {CurrentOffset.LastSequenceNumber} · Last LSN: {CurrentOffset.LastCommitLsn}";

    public BatchingTabViewModel()
    {
        RefillIncomingQueue();
    }

    private static List<SimulatedEvent> BuildSeedEvents()
    {
        string[] ops = ["INS", "UPD", "DEL"];
        var events = new List<SimulatedEvent>();
        for (var i = 0; i < 12; i++)
        {
            var seq = 300 + i;
            var op = ops[i % ops.Length];
            events.Add(new SimulatedEvent(0, seq, $"LSN_{seq}", op, "dbo", "Orders", $"OrderID={i + 1}", $"{op} OrderID={i + 1}"));
        }
        return events;
    }

    private void RefillIncomingQueue()
    {
        IncomingQueue.Clear();
        foreach (var ev in _seedEvents)
            IncomingQueue.Add(ev);
    }

    private bool CanAddNextEventToBatch => IncomingQueue.Count > 0 && CurrentBatchPreview.Count < BatchSize;

    [RelayCommand(CanExecute = nameof(CanAddNextEventToBatch))]
    private void AddNextEventToBatch()
    {
        var ev = IncomingQueue[0];
        IncomingQueue.RemoveAt(0);
        CurrentBatchPreview.Add(ev);
        Log.Insert(0, $"Buffered seq {ev.SequenceNumber} — no ledger/offset change yet.");
        NotifyCommands();
    }

    private bool CanCommitBatch => CurrentBatchPreview.Count > 0;

    [RelayCommand(CanExecute = nameof(CanCommitBatch))]
    private void CommitBatch()
    {
        var pendingLedger = new List<LedgerEntry>();
        long lastSeq = 0;
        var lastLsn = "";
        var skipped = 0;

        foreach (var ev in CurrentBatchPreview)
        {
            var alreadyProcessed =
                Ledger.Any(l => l.PartitionId == ev.PartitionId && l.SequenceNumber == ev.SequenceNumber) ||
                pendingLedger.Any(l => l.PartitionId == ev.PartitionId && l.SequenceNumber == ev.SequenceNumber);

            if (alreadyProcessed)
            {
                skipped++;
                continue;
            }

            pendingLedger.Add(new LedgerEntry(ev.PartitionId, ev.SequenceNumber, ev.CommitLsn, DateTime.Now));
            lastSeq = ev.SequenceNumber;
            lastLsn = ev.CommitLsn;
        }

        foreach (var entry in pendingLedger)
            Ledger.Add(entry);

        if (pendingLedger.Count > 0)
            CurrentOffset = new OffsetEntry(0, lastSeq, lastLsn, DateTime.Now);

        var skippedNote = skipped > 0 ? $", {skipped} duplicate(s) skipped" : "";
        Log.Insert(0, $"COMMIT — {pendingLedger.Count} events written to ledger, offset updated once{skippedNote}.");
        CurrentBatchPreview.Clear();
        NotifyCommands();
    }

    [RelayCommand]
    private void SimulateCrashMidBatch()
    {
        if (CurrentBatchPreview.Count == 0) return;

        Log.Insert(0, $"CRASH — batch of {CurrentBatchPreview.Count} events rolled back. 0 ledger rows written, offset unchanged. Batch will be retried.");
    }

    [RelayCommand]
    private void Reset()
    {
        CurrentBatchPreview.Clear();
        Ledger.Clear();
        Log.Clear();
        CurrentOffset = null;
        RefillIncomingQueue();
        NotifyCommands();
    }

    private void NotifyCommands()
    {
        AddNextEventToBatchCommand.NotifyCanExecuteChanged();
        CommitBatchCommand.NotifyCanExecuteChanged();
    }
}
