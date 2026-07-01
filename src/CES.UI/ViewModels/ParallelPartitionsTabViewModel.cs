using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using CES.UI.Models;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;

namespace CES.UI.ViewModels;

public class PartitionWorkerState : ObservableObject
{
    private readonly List<SimulatedEvent> _seedEvents;
    private readonly Queue<SimulatedEvent> _queue;
    private OffsetEntry? _currentOffset;

    public PartitionWorkerState(int partitionId, List<SimulatedEvent> events)
    {
        PartitionId = partitionId;
        _seedEvents = events;
        _queue = new Queue<SimulatedEvent>(events);
    }

    public int PartitionId { get; }

    public ObservableCollection<LedgerEntry> Ledger { get; } = [];

    public OffsetEntry? CurrentOffset
    {
        get => _currentOffset;
        private set
        {
            if (SetProperty(ref _currentOffset, value))
                OnPropertyChanged(nameof(OffsetSummary));
        }
    }

    public string OffsetSummary => CurrentOffset is null
        ? "seq: — · lsn: —"
        : $"seq: {CurrentOffset.LastSequenceNumber} · lsn: {CurrentOffset.LastCommitLsn}";

    public bool HasMoreEvents => _queue.Count > 0;

    public void ProcessNext()
    {
        if (_queue.Count == 0) return;

        var ev = _queue.Dequeue();
        if (!Ledger.Any(l => l.PartitionId == ev.PartitionId && l.SequenceNumber == ev.SequenceNumber))
        {
            Ledger.Add(new LedgerEntry(ev.PartitionId, ev.SequenceNumber, ev.CommitLsn, DateTime.Now));
            CurrentOffset = new OffsetEntry(ev.PartitionId, ev.SequenceNumber, ev.CommitLsn, DateTime.Now);
        }
    }

    public void Reset()
    {
        _queue.Clear();
        foreach (var ev in _seedEvents)
            _queue.Enqueue(ev);
        Ledger.Clear();
        CurrentOffset = null;
    }
}

public partial class ParallelPartitionsTabViewModel : ObservableObject
{
    private const int PartitionCount = 4;

    public ObservableCollection<PartitionWorkerState> Partitions { get; } = [];

    public ObservableCollection<string> Log { get; } = [];

    public ParallelPartitionsTabViewModel()
    {
        for (var p = 0; p < PartitionCount; p++)
            Partitions.Add(new PartitionWorkerState(p, BuildEvents(p)));
    }

    private static List<SimulatedEvent> BuildEvents(int partitionId)
    {
        string[] ops = ["INS", "UPD", "DEL"];
        var orderId = partitionId * 10 + 1;
        var events = new List<SimulatedEvent>();
        for (var i = 0; i < ops.Length; i++)
        {
            var seq = 100 + i;
            events.Add(new SimulatedEvent(partitionId, seq, $"LSN_{partitionId}_{seq}", ops[i], "dbo", "Orders", $"OrderID={orderId}", ops[i]));
        }
        return events;
    }

    private bool CanProcessNextForAll => Partitions.Any(p => p.HasMoreEvents);

    [RelayCommand(CanExecute = nameof(CanProcessNextForAll))]
    private void ProcessNextForAll()
    {
        foreach (var partition in Partitions.Where(partition => partition.HasMoreEvents))
            partition.ProcessNext();

        Log.Insert(0, "Tick — each partition processed one event independently (in production these run on separate concurrent workers).");
        ProcessNextForAllCommand.NotifyCanExecuteChanged();
    }

    [RelayCommand]
    private void Reset()
    {
        foreach (var partition in Partitions)
            partition.Reset();

        Log.Clear();
        ProcessNextForAllCommand.NotifyCanExecuteChanged();
    }
}
