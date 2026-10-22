using OneCRabbit;

namespace OneCRabbit.Tests;

internal sealed class FakeConnector : IBrokerConnector
{
    private readonly Queue<IBrokerConnection> _connections = new();

    public BrokerSettings? LastSettings { get; private set; }
    public List<BrokerSettings> Settings { get; } = new();

    public FakeConnector(params IBrokerConnection[] connections)
    {
        foreach (var connection in connections)
            _connections.Enqueue(connection);
    }

    public IBrokerConnection Connect(BrokerSettings settings)
    {
        LastSettings = settings;
        Settings.Add(settings);
        if (_connections.Count == 0)
            throw new InvalidOperationException("Нет подготовленного подключения.");
        return _connections.Dequeue();
    }
}

internal sealed class ThrowingConnector : IBrokerConnector
{
    public IBrokerConnection Connect(BrokerSettings settings) =>
        throw new InvalidOperationException("broker down");
}

internal sealed class FakeConnection : IBrokerConnection
{
    public FakeChannel Channel { get; } = new();
    public int CloseCount { get; private set; }

    public event Action? Recovered;

    #pragma warning disable CS0067
    public event Action<string, string>? ConsumerTagChanged;
#pragma warning restore CS0067

    public IBrokerChannel CreateChannel() => Channel;

    public void Close() => CloseCount++;

    public void RaiseRecovered() => Recovered?.Invoke();
}

internal sealed class FakeChannel : IBrokerChannel
{
    public List<(string Queue, bool Durable, string? Type)> Queues { get; } = new();
    public List<string> Existing { get; } = new();
    public List<(string Exchange, string RoutingKey, byte[] Body, bool Persistent, IReadOnlyDictionary<string, string>? Headers)> Published { get; } = new();
    public Queue<byte[]?> Incoming { get; } = new();
    public int CloseCount { get; private set; }
    public ushort? Prefetch { get; private set; }
    public string? ActiveConsumerTag { get; private set; }
    public List<string> Cancelled { get; } = new();
    public List<ulong> Acks { get; } = new();
    public List<(ulong Tag, bool Requeue)> Nacks { get; } = new();

    private Action<Delivery>? _onMessage;

    public void DeclareQueue(string queue, bool durable, IReadOnlyDictionary<string, object>? arguments)
    {
        string? type = null;
        if (arguments != null && arguments.TryGetValue("x-queue-type", out var value))
            type = value as string;

        Queues.Add((queue, durable, type));
    }

    public void DeclareExisting(string queue) => Existing.Add(queue);

    public void Publish(
        string exchange,
        string routingKey,
        byte[] body,
        bool persistent,
        IReadOnlyDictionary<string, string>? headers) =>
        Published.Add((exchange, routingKey, body, persistent, headers));

    public byte[]? TryGetMessage(string queue) => Incoming.Count == 0 ? null : Incoming.Dequeue();

    public void SetPrefetch(ushort prefetchCount) => Prefetch = prefetchCount;

    public string Subscribe(string queue, string consumerTag, Action<Delivery> onMessage)
    {
        _onMessage = onMessage;
        ActiveConsumerTag = string.IsNullOrEmpty(consumerTag) ? "amq.ctag-server" : consumerTag;
        return ActiveConsumerTag;
    }

    public void Unsubscribe(string consumerTag)
    {
        Cancelled.Add(consumerTag);
        ActiveConsumerTag = null;
        _onMessage = null;
    }

    public void Ack(ulong deliveryTag) => Acks.Add(deliveryTag);

    public void Nack(ulong deliveryTag, bool requeue) => Nacks.Add((deliveryTag, requeue));

    public void Deliver(
        ulong deliveryTag,
        byte[] body,
        bool redelivered = false,
        string routingKey = "orders",
        IReadOnlyDictionary<string, string>? headers = null)
    {
        _onMessage?.Invoke(new Delivery(deliveryTag, ActiveConsumerTag ?? "", "", routingKey, redelivered, body, headers));
    }

    public void Close() => CloseCount++;
}
