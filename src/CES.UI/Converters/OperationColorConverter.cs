using System;
using System.Globalization;
using Avalonia.Data.Converters;
using Avalonia.Media;

namespace CES.UI.Converters;

public class OperationColorConverter : IValueConverter
{
    public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        value?.ToString() switch
        {
            "INS" => new SolidColorBrush(Color.Parse("#2D6A4F")),  // dark green
            "UPD" => new SolidColorBrush(Color.Parse("#B45309")),  // amber
            "DEL" => new SolidColorBrush(Color.Parse("#9B1C1C")),  // dark red
            _     => new SolidColorBrush(Color.Parse("#374151"))
        };

    public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture)
        => throw new NotSupportedException();
}
