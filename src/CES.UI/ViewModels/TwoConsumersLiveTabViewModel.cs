using System;
using System.Collections.ObjectModel;
using System.Threading;
using CES.UI.Models;
using CES.UI.Services;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;

namespace CES.UI.ViewModels;

public partial class LiveConsumerState : ObservableObject
{
    private CancellationTokenSource? _cts;

    public LiveConsumerState(string name, string consumerGroup, string databaseName)
    {
        Name = name;
        ConsumerGroup = consumerGroup;
        DatabaseName = databaseName;
    }

    public string Name { get; }

    public string ConsumerGroup { get; }

    public string DatabaseName { get; }

    public string Subtitle => $"group {ConsumerGroup} → {DatabaseName}";

    [ObservableProperty]
    private string _status = "Stopped";

    [ObservableProperty]
    private bool _isRunning;

    [ObservableProperty]
    private int _appliedCount;

    [ObservableProperty]
    private int _duplicateCount;

    [ObservableProperty]
    private string _offsetSummary = "Last seq: — · Last LSN: —";

    public ObservableCollection<LiveApplyEntry> Log { get; } = [];

    private bool CanStart => !IsRunning;

    [RelayCommand(CanExecute = nameof(CanStart))]
    private void Start()
    {
        Log.Clear();
        AppliedCount = 0;
        DuplicateCount = 0;
        Status = "Connecting…";
        IsRunning = true;

        _cts = new CancellationTokenSource();
        new LiveConsumerService(this).Start(_cts.Token);
    }

    [RelayCommand(CanExecute = nameof(IsRunning))]
    private void Stop()
    {
        _cts?.Cancel();
        _cts = null;
        MarkStopped("Stopped");
    }

    partial void OnIsRunningChanged(bool value)
    {
        StartCommand.NotifyCanExecuteChanged();
        StopCommand.NotifyCanExecuteChanged();
    }

    // Called from LiveConsumerService via the UI dispatcher
    public void RecordResult(string operation, int orderId, long sequenceNumber, string commitLsn, bool applied)
    {
        if (applied)
        {
            AppliedCount++;
            OffsetSummary = $"Last seq: {sequenceNumber} · Last LSN: {commitLsn}";
        }
        else
        {
            DuplicateCount++;
        }

        Log.Insert(0, new LiveApplyEntry(DateTime.Now, operation, $"OrderID={orderId}", sequenceNumber, applied));
        if (Log.Count > 200)
            Log.RemoveAt(Log.Count - 1);
    }

    public void MarkStopped(string status)
    {
        Status = status;
        IsRunning = false;
    }
}

public class TwoConsumersLiveTabViewModel
{
    public LiveConsumerState Consumer1 { get; } = new("Consumer 1 — Replication", "consumer1", "CES_Destination1");

    public LiveConsumerState Consumer2 { get; } = new("Consumer 2 — Analytics", "consumer2", "CES_Destination2");
}
