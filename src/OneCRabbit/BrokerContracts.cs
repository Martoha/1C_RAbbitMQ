namespace OneCRabbit;

internal sealed class BrokerSettings
{
    public BrokerSettings(string host, int port, string user, string password, string virtualHost, string clientName)
    {
        Host = host;
        Port = port;
        User = user;
        Password = password;
        VirtualHost = virtualHost;
        ClientName = clientName;
    }

    public string Host { get; }
    public int Port { get; }
    public string User { get; }
    public string Password { get; }
    public string VirtualHost { get; }
    public string ClientName { get; }
}

internal sealed class Delivery
{
    public Delivery(
        ulong deliveryTag,
        string consumerTag,
        string exchange,
        string routingKey,
        bool redelivered,
        byte[] body,
        IReadOnlyDictionary<string, string>? headers = null)
    {
        DeliveryTag = deliveryTag;
        ConsumerTag = consumerTag;
        Exchange = exchange;
        RoutingKey = routingKey;
        Redelivered = redelivered;
        Body = body;
        Headers = HeaderCopy.From(headers);
    }

    public ulong DeliveryTag { get; }
    public string ConsumerTag { get; }
    public string Exchange { get; }
    public string RoutingKey { get; }
    public bool Redelivered { get; }
    public byte[] Body { get; }
    public IReadOnlyDictionary<string, string> Headers { get; }
}

internal interface IBrokerChannel
{
    void DeclareQueue(string queue, bool durable, IReadOnlyDictionary<string, object>? arguments);
    void DeclareExisting(string queue);
    void Publish(string exchange, string routingKey, byte[] body, bool persistent, IReadOnlyDictionary<string, string>? headers);
    byte[]? TryGetMessage(string queue);
    void SetPrefetch(ushort prefetchCount);
    string Subscribe(string queue, string consumerTag, Action<Delivery> onMessage);
    void Unsubscribe(string consumerTag);
    void Ack(ulong deliveryTag);
    void Nack(ulong deliveryTag, bool requeue);
    void Close();
}

internal interface IBrokerConnection
{
    event Action? Recovered;
    event Action<string, string>? ConsumerTagChanged;
    IBrokerChannel CreateChannel();
    void Close();
}

internal interface IBrokerConnector
{
    IBrokerConnection Connect(BrokerSettings settings);
}

internal static class HeaderCopy
{
    internal static Dictionary<string, string> From(IReadOnlyDictionary<string, string>? headers)
    {
        var copy = new Dictionary<string, string>();
        if (headers == null)
            return copy;

        foreach (var header in headers)
            copy.Add(header.Key, header.Value);

        return copy;
    }
}
