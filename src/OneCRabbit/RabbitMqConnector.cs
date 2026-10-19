using System.Reflection;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace OneCRabbit;

internal sealed class RabbitMqConnector : IBrokerConnector
{
    private readonly Func<BrokerSettings, IConnection> _open;

    public RabbitMqConnector()
        : this(Open)
    {
    }

    internal RabbitMqConnector(Func<BrokerSettings, IConnection> open)
    {
        _open = open;
    }

    public IBrokerConnection Connect(BrokerSettings settings) =>
        new RabbitConnection(_open(settings));

    internal static ConnectionFactory CreateFactory(BrokerSettings settings) =>
        new()
        {
            HostName = settings.Host,
            Port = settings.Port,
            UserName = settings.User,
            Password = settings.Password,
            VirtualHost = settings.VirtualHost,
            ClientProvidedName = settings.ClientName,
            AutomaticRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(10)
        };

    private static IConnection Open(BrokerSettings settings) =>
        CreateFactory(settings).CreateConnection();
}

internal sealed class RabbitConnection : IBrokerConnection
{
    private readonly IConnection _connection;

    public RabbitConnection(IConnection connection)
    {
        _connection = connection;
        if (connection is IAutorecoveringConnection recovering)
        {
            recovering.RecoverySucceeded += (_, _) => Recovered?.Invoke();
            recovering.ConsumerTagChangeAfterRecovery += (_, args) =>
                ConsumerTagChanged?.Invoke(args.TagBefore, args.TagAfter);
        }
    }

    public event Action? Recovered;

    public event Action<string, string>? ConsumerTagChanged;

    public IBrokerChannel CreateChannel() => new RabbitChannel(_connection.CreateModel());

    public void Close()
    {
        try
        {
            if (_connection.IsOpen)
                _connection.Close();
        }
        finally
        {
            _connection.Dispose();
        }
    }
}

internal sealed class RabbitChannel : IBrokerChannel
{
    private readonly IModel _model;

    public RabbitChannel(IModel model)
    {
        _model = model;
    }

    public void DeclareQueue(string queue, bool durable, IReadOnlyDictionary<string, object>? arguments) =>
        _model.QueueDeclare(queue, durable, exclusive: false, autoDelete: false, arguments?.ToDictionary(item => item.Key, item => item.Value));

    public void DeclareExisting(string queue)
    {
        try
        {
            PassiveDeclare.Invoke(_model, new object[] { queue });
        }
        catch (TargetInvocationException exception) when (exception.InnerException != null)
        {
            throw exception.InnerException;
        }
    }

    public void Publish(string exchange, string routingKey, byte[] body, bool persistent)
    {
        var properties = _model.CreateBasicProperties();
        properties.Persistent = persistent;
        _model.BasicPublish(exchange, routingKey, mandatory: false, basicProperties: properties, body: body);
    }

    public byte[]? TryGetMessage(string queue)
    {
        var result = _model.BasicGet(queue, autoAck: true);
        return result?.Body.ToArray();
    }

    public void SetPrefetch(ushort prefetchCount) =>
        _model.BasicQos(0, prefetchCount, global: false);

    public string Subscribe(string queue, string consumerTag, Action<Delivery> onMessage)
    {
        var consumer = new EventingBasicConsumer(_model);
        consumer.Received += (_, delivery) =>
        {
            onMessage(new Delivery(
                delivery.DeliveryTag,
                delivery.ConsumerTag,
                delivery.Exchange,
                delivery.RoutingKey,
                delivery.Redelivered,
                delivery.Body.ToArray()));
        };

        return _model.BasicConsume(queue, autoAck: false, consumerTag, consumer);
    }

    public void Unsubscribe(string consumerTag) => _model.BasicCancel(consumerTag);

    public void Ack(ulong deliveryTag) => _model.BasicAck(deliveryTag, multiple: false);

    public void Nack(ulong deliveryTag, bool requeue) =>
        _model.BasicNack(deliveryTag, multiple: false, requeue);

    public void Close()
    {
        try
        {
            if (_model.IsOpen)
                _model.Close();
        }
        finally
        {
            _model.Dispose();
        }
    }

    private static readonly MethodInfo PassiveDeclare = typeof(IModel)
        .GetMethods()
        .Single(method =>
        {
            var parameters = method.GetParameters();
            return parameters.Length == 1
                && parameters[0].ParameterType == typeof(string)
                && method.ReturnType == typeof(QueueDeclareOk);
        });
}
