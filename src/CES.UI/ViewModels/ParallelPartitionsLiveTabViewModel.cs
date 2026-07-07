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

public record PartitionRow(long SequenceNumber, string Op, string CommitLsn);

public partial class LivePartitionWorkerState : ObservableObject
{
    private readonly Queue<PendingLiveEvent> _queue = new();

    public LivePartitionWorkerState(int partitionId) => PartitionId = partitionId;

    public int PartitionId { get; }

    public ObservableCollection<PartitionRow> Rows { get; } = [];

    [ObservableProperty]
    private int _queuedCount;

    [ObservableProperty]
    private OffsetEntry? _currentOffset;

    partial void OnCurrentOffsetChanged(OffsetEntry? value) => OnPropertyChanged(nameof(OffsetSummary));

    public string OffsetSummary => CurrentOffset is null
        ? "seq: — · lsn: —"
        : $"seq: {CurrentOffset.LastSequenceNumber} · lsn: {CurrentOffset.LastCommitLsn}";

    public void Enqueue(PendingLiveEvent pending)
    {
        _queue.Enqueue(pending);
        QueuedCount = _queue.Count;
    }

    public PendingLiveEvent Dequeue()
    {
        var pending = _queue.Dequeue();
        QueuedCount = _queue.Count;
        return pending;
    }

    public void RecordResult(PendingLiveEvent pending, bool applied)
    {
        Rows.Add(new PartitionRow(pending.SequenceNumber, applied ? pending.Event.Operation : "DUP", pending.Event.CommitLsn));
        if (applied)
            CurrentOffset = new OffsetEntry(pending.PartitionId, pending.SequenceNumber, pending.Event.CommitLsn, DateTime.Now);
    }

    public void Clear()
    {
        _queue.Clear();
        QueuedCount = 0;
        Rows.Clear();
        CurrentOffset = null;
    }
}

/// <summary>
/// Live version of the Parallel Partitions tab: the orders hub has 4 partitions,
/// and every "Process Next Event (all partitions)" tick lets each partition worker
/// apply one event to CES_Partitions concurrently — per-partition ledgers/offsets
/// (partition_id is part of both keys) keep the workers fully independent.
/// </summary>
public partial class ParallelPartitionsLiveTabViewModel : ObservableObject
{
    private const int PartitionCount = 4;

    private CancellationTokenSource? _cts;

    public ObservableCollection<LivePartitionWorkerState> Partitions { get; } = [];

    public ObservableCollection<string> Log { get; } = [];

    [ObservableProperty]
    private string _status = "Stopped";

    [ObservableProperty]
    private bool _isRunning;

    public ParallelPartitionsLiveTabViewModel()
    {
        for (var p = 0; p < PartitionCount; p++)
            Partitions.Add(new LivePartitionWorkerState(p));
    }

    private bool CanStart => !IsRunning;

    [RelayCommand(CanExecute = nameof(CanStart))]
    private void Start()
    {
        foreach (var partition in Partitions)
            partition.Clear();
        Log.Clear();
        Status = "Connecting…";
        IsRunning = true;

        _cts = new CancellationTokenSource();
        new ParallelPartitionsLiveService(this).Start(_cts.Token);
        ProcessNextForAllCommand.NotifyCanExecuteChanged();
    }

    [RelayCommand(CanExecute = nameof(IsRunning))]
    private void Stop()
    {
        _cts?.Cancel();
        _cts = null;
        MarkStopped("Stopped");
    }

    private bool CanProcessNextForAll => IsRunning && Partitions.Any(p => p.QueuedCount > 0);

    [RelayCommand(CanExecute = nameof(CanProcessNextForAll))]
    private async Task ProcessNextForAll()
    {
        var work = Partitions.Where(p => p.QueuedCount > 0)
                             .Select(p => (State: p, Pending: p.Dequeue()))
                             .ToList();

        try
        {
            // One concurrent apply per partition — this is the parallel part
            var tasks = work
                .Select(async w => (w.State, w.Pending,
                    Applied: await ParallelPartitionsLiveService.ApplyAsync(w.Pending, CancellationToken.None)))
                .ToList();

            foreach (var task in tasks)
            {
                var (state, pending, applied) = await task;
                state.RecordResult(pending, applied);
            }

            Log.Insert(0, $"Tick — {work.Count} partition worker(s) each applied one event concurrently.");
            if (Log.Count > 4)
                Log.RemoveAt(Log.Count - 1);
        }
        catch (Exception ex)
        {
            Log.Insert(0, $"Tick failed: {ex.Message}");
        }
        ProcessNextForAllCommand.NotifyCanExecuteChanged();
    }

    // Clears the destination database; stop + start afterwards to replay the
    // stream and watch all four workers fill up again.
    [RelayCommand]
    private async Task ResetDb()
    {
        try
        {
            await ParallelPartitionsLiveService.ResetDatabaseAsync(CancellationToken.None);
            foreach (var partition in Partitions)
            {
                partition.Rows.Clear();
                partition.CurrentOffset = null;
            }
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
        ProcessNextForAllCommand.NotifyCanExecuteChanged();
    }

    // Called from ParallelPartitionsLiveService via the UI dispatcher
    public void EnqueuePending(PendingLiveEvent pending)
    {
        var state = Partitions.FirstOrDefault(p => p.PartitionId == pending.PartitionId);
        if (state is null) return;

        state.Enqueue(pending);
        ProcessNextForAllCommand.NotifyCanExecuteChanged();
    }

    public void InitializeFromDb(List<LedgerEntry> ledger, List<OffsetEntry> offsets)
    {
        foreach (var partition in Partitions)
        {
            partition.Rows.Clear();
            foreach (var entry in ledger.Where(l => l.PartitionId == partition.PartitionId))
                partition.Rows.Add(new PartitionRow(entry.SequenceNumber, "—", entry.CommitLsn));
            partition.CurrentOffset = offsets.FirstOrDefault(o => o.PartitionId == partition.PartitionId);
        }
        Log.Insert(0, $"Loaded {ledger.Count} existing ledger row(s) from {ParallelPartitionsLiveService.DatabaseName}.");
    }

    public void MarkStopped(string status)
    {
        Status = status;
        IsRunning = false;
    }
}
