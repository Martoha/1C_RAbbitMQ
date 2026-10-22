using System.Runtime.InteropServices;

namespace OneCRabbit;

[ComVisible(true)]
[Guid("3A9C1E72-5D44-4B18-9F60-8C2E7A1B4D55")]
[ClassInterface(ClassInterfaceType.AutoDual)]
public sealed class RabbitMessage
{
    public RabbitMessage()
    {
    }

    internal RabbitMessage(
        string body,
        string deliveryTag,
        string consumerTag,
        string exchange,
        string routingKey,
        bool redelivered,
        IReadOnlyDictionary<string, string>? headers)
    {
        HasMessage = true;
        Body = body;
        DeliveryTag = deliveryTag;
        ConsumerTag = consumerTag;
        Exchange = exchange;
        RoutingKey = routingKey;
        Redelivered = redelivered;
        _headers = HeaderCopy.From(headers);
    }

    private readonly Dictionary<string, string> _headers = new();

    public bool HasMessage { get; }

    public string Body { get; } = "";

    public string DeliveryTag { get; } = "";

    public string ConsumerTag { get; } = "";

    public string Exchange { get; } = "";

    public string RoutingKey { get; } = "";

    public bool Redelivered { get; }

    public string Header(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            return "";

        return _headers.TryGetValue(name, out var value) ? value : "";
    }

    public string HeaderNames() => string.Join(", ", _headers.Keys);
}
