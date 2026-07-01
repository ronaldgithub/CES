using System.Threading;
using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Markup.Xaml;
using CES.UI.Services;
using CES.UI.ViewModels;
using CES.UI.Views;

namespace CES.UI;

public partial class App : Application
{
    private readonly CancellationTokenSource _cts = new();

    public override void Initialize()
    {
        AvaloniaXamlLoader.Load(this);
    }

    public override void OnFrameworkInitializationCompleted()
    {
        if (ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop)
        {
            var viewModel = new MainWindowViewModel();
            desktop.MainWindow = new MainWindow { DataContext = viewModel };
            desktop.Exit += (_, _) => _cts.Cancel();

            var consumer = new KafkaConsumerService(viewModel);
            consumer.Start(_cts.Token);
        }

        base.OnFrameworkInitializationCompleted();
    }
}
