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
}
