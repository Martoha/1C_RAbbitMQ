using System.Runtime.InteropServices;

namespace OneCRabbit;

[ComVisible(true)]
[Guid("8F5C2A41-6B1E-4E0A-9C3D-2A7B6E5D4C31")]
[ProgId("OneCRabbit.Client")]
[ClassInterface(ClassInterfaceType.AutoDual)]
public sealed class RabbitClient
{
    private readonly RabbitSession _session;

    static RabbitClient() => DependencyResolver.Register();

    public RabbitClient()
        : this(new RabbitMqConnector())
    {
    }

    internal RabbitClient(IBrokerConnector connector)
    {
        _session = new RabbitSession(connector);
    }

    public bool IsConnected() => _session.IsConnected;

    public void SetConnectionName(string name) => _session.SetConnectionName(name);

    public void SetPrefetch(int count) => _session.SetPrefetch(count);

    public void SetReadEncoding(string name) => _session.SetReadEncoding(name);

    public void SetWriteEncoding(string name) => _session.SetWriteEncoding(name);

    public string ReadEncoding() => _session.ReadEncodingName;

    public string WriteEncoding() => _session.WriteEncodingName;

    public void Connect(string host, int port, string user, string password, string vhost) =>
        _session.Connect(host, port, user, password, vhost);

    public void DeclareQueue(string queue) => _session.DeclareQueue(queue);

    public void DeclareQueueType(string queue, string queueType) => _session.DeclareQueue(queue, queueType);

    public void DeclareExisting(string queue) => _session.DeclareExisting(queue);

    public void Publish(string queue, string body) => _session.Publish(queue, body);

    public void PublishTo(string exchange, string routingKey, string body) =>
        _session.PublishTo(exchange, routingKey, body);

    public string GetMessage(string queue) => _session.GetMessage(queue);

    public string Subscribe(string queue, string consumerTag) => _session.Subscribe(queue, consumerTag);

    public void Unsubscribe() => _session.Unsubscribe();

    public RabbitMessage Receive() => _session.Receive();

    public int PendingCount() => _session.PendingCount();

    public int UnackedCount() => _session.UnackedCount();

    public void Ack(string deliveryTag) => _session.Ack(deliveryTag);

    public void Nack(string deliveryTag, bool requeue) => _session.Nack(deliveryTag, requeue);

    public void Move(string deliveryTag, string queue) => _session.Move(deliveryTag, queue);

    public void Disconnect() => _session.Disconnect();
}
