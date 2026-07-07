using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CES.UI.Models;
using CES.UI.Services;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;

namespace CES.UI.ViewModels;

/// <summary>
/// Live version of the Batching tab: real events queue up from the Event Hub
/// (consumer group "batching"), the user buffers up to 5 into a batch, and
/// "Commit Batch" applies them in one SQL transaction to CES_Batching —
/// ledger row per event, offset updated once. "Simulate Crash Mid-Batch" throws
/// the uncommitted batch away, proving nothing was persisted before the commit.
/// </summary>
public partial class BatchingLiveTabViewModel : ObservableObject
{
    private const int BatchSize = 5;

    private CancellationTokenSource? _cts;

    public ObservableCollection<PendingLiveEvent> IncomingQueue { get; } = [];

    public ObservableCollection<PendingLiveEvent> CurrentBatch { get; } = [];

    public ObservableCollection<LedgerEntry> Ledger { get; } = [];

    public ObservableCollection<string> Log { get; } = [];

    [ObservableProperty]
    private string _status = "Stopped";

    [ObservableProperty]
    private bool _isRunning;

    [ObservableProperty]
    private OffsetEntry? _currentOffset;

    partial void OnCurrentOffsetChanged(OffsetEntry? value) => OnPropertyChanged(nameof(OffsetSummary));

    public string OffsetSummary => CurrentOffset is null
        ? "Last seq: — · Last LSN: —"
        : $"Last seq: {CurrentOffset.LastSequenceNumber} · Last LSN: {CurrentOffset.LastCommitLsn}";

    [ObservableProperty]
    private string _targetTableSummary = "(not connected)";

    private bool CanStart => !IsRunning;

    [RelayCommand(CanExecute = nameof(CanStart))]
    private void Start()
    {
        IncomingQueue.Clear();
        CurrentBatch.Clear();
        Ledger.Clear();
        Log.Clear();
        CurrentOffset = null;
        Status = "Connecting…";
        IsRunning = true;

        _cts = new CancellationTokenSource();
        new BatchingLiveService(this).Start(_cts.Token);
        NotifyCommands();
    }

    [RelayCommand(CanExecute = nameof(IsRunning))]
    private void Stop()
    {
        _cts?.Cancel();
        _cts = null;
        MarkStopped("Stopped");
    }

    private bool CanAddNextEventToBatch => IsRunning && IncomingQueue.Count > 0 && CurrentBatch.Count < BatchSize;

    [RelayCommand(CanExecute = nameof(CanAddNextEventToBatch))]
    private void AddNextEventToBatch()
    {
        var pending = IncomingQueue[0];
        IncomingQueue.RemoveAt(0);
        CurrentBatch.Add(pending);
        Log.Insert(0, $"Buffered seq {pending.SequenceNumber} — no SQL yet: ledger and offset unchanged.");
        NotifyCommands();
    }

    private bool CanCommitBatch => CurrentBatch.Count > 0;

    [RelayCommand(CanExecute = nameof(CanCommitBatch))]
    private async Task CommitBatch()
    {
        var batch = CurrentBatch.ToList();
        try
        {
            var (results, orderRows) = await BatchingLiveService.CommitBatchAsync(batch, CancellationToken.None);

            var applied = 0;
            var skipped = 0;
            foreach (var (pending, wasApplied) in results)
            {
                if (wasApplied)
                {
                    applied++;
                    Ledger.Add(new LedgerEntry(pending.PartitionId, pending.SequenceNumber, pending.Event.CommitLsn, DateTime.Now));
                    CurrentOffset = new OffsetEntry(pending.PartitionId, pending.SequenceNumber, pending.Event.CommitLsn, DateTime.Now);
                }
                else
                {
                    skipped++;
                }
            }

            TargetTableSummary = $"dbo.Orders: {orderRows} row(s)";
            var skippedNote = skipped > 0 ? $", {skipped} duplicate(s) skipped" : "";
            Log.Insert(0, $"COMMIT — one transaction: {applied} events applied + ledger rows, offset updated once{skippedNote}.");
            CurrentBatch.Clear();
        }
        catch (Exception ex)
        {
            Log.Insert(0, $"Commit failed, transaction rolled back — nothing persisted: {ex.Message}");
        }
        NotifyCommands();
    }

    private bool CanSimulateCrashMidBatch => CurrentBatch.Count > 0;

    [RelayCommand(CanExecute = nameof(CanSimulateCrashMidBatch))]
    private void SimulateCrashMidBatch()
    {
        var lost = CurrentBatch.Count;
        CurrentBatch.Clear();
        Log.Insert(0, $"CRASH — batch of {lost} events lost before commit: 0 ledger rows written, offset unchanged. " +
                      "Stop + Start replays the stream and the events come back.");
        NotifyCommands();
    }

    // Clears the destination database; stop + start afterwards to replay the
    // stream and watch the batches get applied (not skipped) again.
    [RelayCommand]
    private async Task ResetDb()
    {
        try
        {
            await BatchingLiveService.ResetDatabaseAsync(CancellationToken.None);
            Ledger.Clear();
            CurrentOffset = null;
            TargetTableSummary = "dbo.Orders: 0 row(s)";
            Log.Insert(0, "Database reset: ces_ledger, ces_offsets and Orders emptied. Stop + Start to replay the stream.");
        }
        catch (Exception ex)
        {
            Log.Insert(0, $"Reset failed: {ex.Message}");
        }
    }

    partial void OnIsRunningChanged(bool value) => NotifyCommands();

    private void NotifyCommands()
    {
        StartCommand.NotifyCanExecuteChanged();
        StopCommand.NotifyCanExecuteChanged();
        AddNextEventToBatchCommand.NotifyCanExecuteChanged();
        CommitBatchCommand.NotifyCanExecuteChanged();
        SimulateCrashMidBatchCommand.NotifyCanExecuteChanged();
    }

    // Called from BatchingLiveService via the UI dispatcher
    public void EnqueueIncoming(PendingLiveEvent pending)
    {
        IncomingQueue.Add(pending);
        NotifyCommands();
    }

    public void InitializeFromDb(List<LedgerEntry> ledger, OffsetEntry? offset)
    {
        Ledger.Clear();
        foreach (var entry in ledger)
            Ledger.Add(entry);
        CurrentOffset = offset;
        TargetTableSummary = ledger.Count == 0 ? "(no rows applied yet)" : "(loaded from previous run)";
        Log.Insert(0, $"Loaded {ledger.Count} existing ledger row(s) from {BatchingLiveService.DatabaseName}.");
    }

    public void MarkStopped(string status)
    {
        Status = status;
        IsRunning = false;
    }
}
