using System.Collections.ObjectModel;
using CES.UI.Models;
using CommunityToolkit.Mvvm.ComponentModel;

namespace CES.UI.ViewModels;

public partial class MainWindowViewModel : ObservableObject
{
    [ObservableProperty]
    private string _status = "Connecting to Kafka...";

    [ObservableProperty]
    private int _eventCount;

    public ObservableCollection<ChangeEvent> Events { get; } = [];

    public IdempotencyTabViewModel IdempotencyTab { get; } = new();

    public IdempotencyLiveTabViewModel IdempotencyLiveTab { get; } = new();

    public TwoConsumersTabViewModel TwoConsumersTab { get; } = new();

    public TwoConsumersLiveTabViewModel TwoConsumersLiveTab { get; } = new();

    public ParallelPartitionsTabViewModel ParallelPartitionsTab { get; } = new();

    public MultiTableTabViewModel MultiTableTab { get; } = new();

    public BatchingTabViewModel BatchingTab { get; } = new();
}
