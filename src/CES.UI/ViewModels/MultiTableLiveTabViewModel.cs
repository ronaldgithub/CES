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
/// Live version of the Multi-Table Routing tab: real Orders and OrderLines events
/// from consumer group "multitable" buffer in a queue, and each "Process Next
/// Event" click routes one of them to the matching table in CES_MultiTable.
/// One shared ces_ledger/ces_offsets guards both tables.
/// </summary>
public partial class MultiTableLiveTabViewModel : ObservableObject
{
    private readonly Queue<PendingEnvelopeEvent> _pending = new();
    private CancellationTokenSource? _cts;

    public ObservableCollection<string> OrdersRows { get; } = [];

    public ObservableCollection<string> OrderLinesRows { get; } = [];

    public ObservableCollection<string> Log { get; } = [];

    [ObservableProperty]
    private string _status = "Stopped";

    [ObservableProperty]
    private bool _isRunning;

    [ObservableProperty]
    private int _ledgerCount;

    [ObservableProperty]
    private OffsetEntry? _currentOffset;

    partial void OnCurrentOffsetChanged(OffsetEntry? value) => OnPropertyChanged(nameof(OffsetSummary));

    public string OffsetSummary => CurrentOffset is null
        ? "Partition: — · Last seq: — · Last LSN: —"
        : $"Partition: {CurrentOffset.PartitionId} · Last seq: {CurrentOffset.LastSequenceNumber} · Last LSN: {CurrentOffset.LastCommitLsn}";

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
            return $"Next: {next.Display}{more}";
        }
    }

    private bool CanStart => !IsRunning;

    [RelayCommand(CanExecute = nameof(CanStart))]
    private void Start()
    {
        _pending.Clear();
        PendingCount = 0;
        OrdersRows.Clear();
        OrderLinesRows.Clear();
        Log.Clear();
        LedgerCount = 0;
        CurrentOffset = null;
        Status = "Connecting…";
        IsRunning = true;

        _cts = new CancellationTokenSource();
        new MultiTableLiveService(this).Start(_cts.Token);
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
            var applied = await MultiTableLiveService.ApplyAsync(pending, CancellationToken.None);

            if (applied)
            {
                LedgerCount++;
                CurrentOffset = new OffsetEntry(pending.PartitionId, pending.SequenceNumber, pending.Envelope.CommitLsn, DateTime.Now);
                Log.Insert(0, $"seq {pending.SequenceNumber} — routed to dbo.{pending.Envelope.Table}. Shared ledger + offset updated.");
                await RefreshTables();
            }
            else
            {
                Log.Insert(0, $"seq {pending.SequenceNumber} — DUPLICATE, already in shared ledger. Skipped.");
            }
        }
        catch (Exception ex)
        {
            Log.Insert(0, $"seq {pending.SequenceNumber} — SQL error: {ex.Message}");
        }
    }

    // Clears the destination database; stop + start afterwards to replay the
    // stream and watch the routing happen again.
    [RelayCommand]
    private async Task ResetDb()
    {
        try
        {
            await MultiTableLiveService.ResetDatabaseAsync(CancellationToken.None);
            OrdersRows.Clear();
            OrderLinesRows.Clear();
            LedgerCount = 0;
            CurrentOffset = null;
            Log.Insert(0, "Database reset: shared ledger/offsets, Orders and OrderLines emptied. Stop + Start to replay the stream.");
        }
        catch (Exception ex)
        {
            Log.Insert(0, $"Reset failed: {ex.Message}");
        }
    }

    private async Task RefreshTables()
    {
        var (orders, orderLines) = await MultiTableLiveService.LoadTablesAsync(CancellationToken.None);
        OrdersRows.Clear();
        foreach (var row in orders) OrdersRows.Add(row);
        OrderLinesRows.Clear();
        foreach (var row in orderLines) OrderLinesRows.Add(row);
    }

    partial void OnIsRunningChanged(bool value)
    {
        StartCommand.NotifyCanExecuteChanged();
        StopCommand.NotifyCanExecuteChanged();
        ProcessNextEventCommand.NotifyCanExecuteChanged();
        OnPropertyChanged(nameof(NextEventPreview));
    }

    // Called from MultiTableLiveService via the UI dispatcher
    public void EnqueuePending(PendingEnvelopeEvent pending)
    {
        _pending.Enqueue(pending);
        PendingCount = _pending.Count;
    }

    public void InitializeFromDb(int ledgerCount, OffsetEntry? offset, List<string> orders, List<string> orderLines)
    {
        LedgerCount = ledgerCount;
        CurrentOffset = offset;
        OrdersRows.Clear();
        foreach (var row in orders) OrdersRows.Add(row);
        OrderLinesRows.Clear();
        foreach (var row in orderLines) OrderLinesRows.Add(row);
        Log.Insert(0, $"Loaded {ledgerCount} existing ledger row(s) from {MultiTableLiveService.DatabaseName}.");
    }

    public void MarkStopped(string status)
    {
        Status = status;
        IsRunning = false;
    }
}
