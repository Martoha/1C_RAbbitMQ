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
        bool redelivered)
    {
        HasMessage = true;
        Body = body;
        DeliveryTag = deliveryTag;
        ConsumerTag = consumerTag;
        Exchange = exchange;
        RoutingKey = routingKey;
        Redelivered = redelivered;
    }

    public bool HasMessage { get; }

    public string Body { get; } = "";

    public string DeliveryTag { get; } = "";

    public string ConsumerTag { get; } = "";

    public string Exchange { get; } = "";

    public string RoutingKey { get; } = "";

    public bool Redelivered { get; }
}
