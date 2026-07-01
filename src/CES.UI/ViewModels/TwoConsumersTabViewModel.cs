using System;
using System.Collections.ObjectModel;
using System.Linq;
using CES.UI.Models;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;

namespace CES.UI.ViewModels;

public class ConsumerState : ObservableObject
{
    private readonly int _startIndex;
    private int _cursor;
    private OffsetEntry? _currentOffset;

    public ConsumerState(string name, int startIndex)
    {
        Name = name;
        _startIndex = startIndex;
        _cursor = startIndex;
    }

    public string Name { get; }

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
        ? "Last seq: — · Last LSN: —"
        : $"Last seq: {CurrentOffset.LastSequenceNumber} · Last LSN: {CurrentOffset.LastCommitLsn}";

    public bool HasMoreEvents(int streamLength) => _cursor < streamLength;

    public void ProcessNext(SimulatedEvent[] stream)
    {
        if (_cursor >= stream.Length) return;

        var ev = stream[_cursor];
        _cursor++;

        if (!Ledger.Any(l => l.PartitionId == ev.PartitionId && l.SequenceNumber == ev.SequenceNumber))
        {
            Ledger.Add(new LedgerEntry(ev.PartitionId, ev.SequenceNumber, ev.CommitLsn, DateTime.Now));
            CurrentOffset = new OffsetEntry(ev.PartitionId, ev.SequenceNumber, ev.CommitLsn, DateTime.Now);
        }
    }

    public void ResetState()
    {
        _cursor = _startIndex;
        Ledger.Clear();
        CurrentOffset = null;
    }
}

public partial class TwoConsumersTabViewModel : ObservableObject
{
    private static readonly SimulatedEvent[] Stream =
    [
        new(0, 100, "LSN_500", "INS", "dbo", "Orders", "OrderID=1", "insert"),
        new(0, 101, "LSN_501", "UPD", "dbo", "Orders", "OrderID=1", "update"),
        new(0, 102, "LSN_502", "UPD", "dbo", "Orders", "OrderID=1", "update"),
        new(0, 103, "LSN_503", "DEL", "dbo", "Orders", "OrderID=1", "delete"),
        new(0, 104, "LSN_504", "INS", "dbo", "Orders", "OrderID=2", "insert")
    ];

    public ConsumerState ConsumerA { get; } = new("Consumer A — Replication", startIndex: 0);

    public ConsumerState ConsumerB { get; } = new("Consumer B — Analytics", startIndex: 2);

    private bool CanAdvanceConsumerA => ConsumerA.HasMoreEvents(Stream.Length);

    private bool CanAdvanceConsumerB => ConsumerB.HasMoreEvents(Stream.Length);

    [RelayCommand(CanExecute = nameof(CanAdvanceConsumerA))]
    private void AdvanceConsumerA()
    {
        ConsumerA.ProcessNext(Stream);
        AdvanceConsumerACommand.NotifyCanExecuteChanged();
    }

    [RelayCommand(CanExecute = nameof(CanAdvanceConsumerB))]
    private void AdvanceConsumerB()
    {
        ConsumerB.ProcessNext(Stream);
        AdvanceConsumerBCommand.NotifyCanExecuteChanged();
    }

    [RelayCommand]
    private void Reset()
    {
        ConsumerA.ResetState();
        ConsumerB.ResetState();
        AdvanceConsumerACommand.NotifyCanExecuteChanged();
        AdvanceConsumerBCommand.NotifyCanExecuteChanged();
    }
}
