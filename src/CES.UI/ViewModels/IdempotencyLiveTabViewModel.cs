using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading;
using System.Threading.Tasks;
using CES.UI.Models;
using CES.UI.Services;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;

namespace CES.UI.ViewModels;

/// <summary>
/// Live version of the Idempotency & Offsets tab: same layout, but the events come
/// from the real Event Hub (consumer group "idempotency") and each click of
/// "Process Next Event" applies exactly one of them to the local
/// CES_IdempotencyDemo database. Incoming events buffer in a queue until clicked.
/// </summary>
public partial class IdempotencyLiveTabViewModel : ObservableObject
{
    private readonly Queue<PendingLiveEvent> _pending = new();
    private CancellationTokenSource? _cts;

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
        ? "Partition: — · Last seq: — · Last LSN: —"
        : $"Partition: {CurrentOffset.PartitionId} · Last seq: {CurrentOffset.LastSequenceNumber} · Last LSN: {CurrentOffset.LastCommitLsn}";

    [ObservableProperty]
    private string _targetTableSummary = "(not connected)";

    [ObservableProperty]
    [NotifyCanExecuteChangedFor(nameof(ProcessNextEventCommand))]
    [NotifyPropertyChangedFor(nameof(NextEventPreview))]
    private int _pendingCount;

    public string NextEventPreview
    {
        get
        {
            if (!IsRunning) return "Not connected.";
            if (_pending.Count == 0) return "Waiting for events…";
            var next = _pending.Peek();
            var more = _pending.Count > 1 ? $"  (+{_pending.Count - 1} more queued)" : "";
            return $"Next: seq {next.SequenceNumber} {next.Event.Operation} OrderID={next.Event.OrderId}{more}";
        }
    }

    private bool CanStart => !IsRunning;

    [RelayCommand(CanExecute = nameof(CanStart))]
    private void Start()
    {
        _pending.Clear();
        PendingCount = 0;
        Ledger.Clear();
        Log.Clear();
        CurrentOffset = null;
        Status = "Connecting…";
        IsRunning = true;

        _cts = new CancellationTokenSource();
        new IdempotencyLiveService(this).Start(_cts.Token);
    }

    [RelayCommand(CanExecute = nameof(IsRunning))]
    private void Stop()
    {
        _cts?.Cancel();
        _cts = null;
        MarkStopped("Stopped");
    }

    private bool CanProcessNextEvent => IsRunning && PendingCount > 0;

    [RelayCommand(CanExecute = nameof(CanProcessNextEvent))]
    private async Task ProcessNextEvent()
    {
        var pending = _pending.Dequeue();
        PendingCount = _pending.Count;

        try
        {
            var (applied, orderRows) = await IdempotencyLiveService.ApplyAsync(pending, CancellationToken.None);

            TargetTableSummary = $"dbo.Orders: {orderRows} row(s)";
            if (applied)
            {
                Ledger.Add(new LedgerEntry(pending.PartitionId, pending.SequenceNumber, pending.Event.CommitLsn, DateTime.Now));
                CurrentOffset = new OffsetEntry(pending.PartitionId, pending.SequenceNumber, pending.Event.CommitLsn, DateTime.Now);
                Log.Insert(0, $"seq {pending.SequenceNumber} {pending.Event.Operation} OrderID={pending.Event.OrderId} — applied. Ledger + offset updated.");
            }
            else
            {
                Log.Insert(0, $"seq {pending.SequenceNumber} — DUPLICATE, already in ces_ledger. Skipped: no DML, no ledger insert, no offset update.");
            }
        }
        catch (Exception ex)
        {
            Log.Insert(0, $"seq {pending.SequenceNumber} — SQL error: {ex.Message}");
        }
    }

    // Clears the destination database; stop + start afterwards to replay the
    // stream and watch everything get applied (not skipped) again.
    [RelayCommand]
    private async Task ResetDb()
    {
        try
        {
            await IdempotencyLiveService.ResetDatabaseAsync(CancellationToken.None);
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

    partial void OnIsRunningChanged(bool value)
    {
        StartCommand.NotifyCanExecuteChanged();
        StopCommand.NotifyCanExecuteChanged();
        ProcessNextEventCommand.NotifyCanExecuteChanged();
        OnPropertyChanged(nameof(NextEventPreview));
    }

    // Called from IdempotencyLiveService via the UI dispatcher
    public void EnqueuePending(PendingLiveEvent pending)
    {
        _pending.Enqueue(pending);
        PendingCount = _pending.Count;
    }

    public void InitializeFromDb(List<LedgerEntry> ledger, OffsetEntry? offset)
    {
        Ledger.Clear();
        foreach (var entry in ledger)
            Ledger.Add(entry);
        CurrentOffset = offset;
        TargetTableSummary = ledger.Count == 0 ? "(no rows applied yet)" : "(loaded from previous run)";
        Log.Insert(0, $"Loaded {ledger.Count} existing ledger row(s) from CES_IdempotencyDemo.");
    }

    public void MarkStopped(string status)
    {
        Status = status;
        IsRunning = false;
    }
}
