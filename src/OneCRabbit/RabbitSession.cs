using System.Text;

namespace OneCRabbit;

internal sealed class RabbitSession
{
    private readonly IBrokerConnector _connector;
    private readonly object _gate = new();
    private readonly object _inboxGate = new();
    private readonly Queue<PendingMessage> _inbox = new();
    private readonly Dictionary<ulong, PendingMessage> _unacked = new();
    private IBrokerConnection? _connection;
    private IBrokerChannel? _channel;
    private string _connectionName = "1C";
    private Encoding _readEncoding = BodyEncoding.Utf8;
    private Encoding _writeEncoding = BodyEncoding.Utf8;
    private ushort _prefetch;
    private string? _consumerTag;

    internal RabbitSession(IBrokerConnector connector)
    {
        _connector = connector;
    }

    internal void SetConnectionName(string? name)
    {
        _connectionName = string.IsNullOrWhiteSpace(name) ? "1C" : name!.Trim();
    }

    internal void SetReadEncoding(string name) => _readEncoding = BodyEncoding.Resolve(name);

    internal void SetWriteEncoding(string name) => _writeEncoding = BodyEncoding.Resolve(name);

    internal string ReadEncodingName => BodyEncoding.CanonicalName(_readEncoding);

    internal string WriteEncodingName => BodyEncoding.CanonicalName(_writeEncoding);

    internal void SetPrefetch(int count)
    {
        if (count is < 0 or > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(count), "Prefetch должен быть от 0 до 65535. 0 означает все сообщения.");

        _prefetch = (ushort)count;
        lock (_gate)
        {
            if (_channel != null)
                _channel.SetPrefetch(_prefetch);
        }
    }
    internal bool IsConnected
    {
        get
        {
            lock (_gate)
                return _channel != null;
        }
    }

    internal void Connect(string host, int port, string user, string password, string vhost)
    {
        if (string.IsNullOrWhiteSpace(host))
            throw new ArgumentException("Не задан адрес сервера RabbitMQ.", nameof(host));
        if (port is < 1 or > 65535)
            throw new ArgumentOutOfRangeException(nameof(port), "Порт должен быть от 1 до 65535.");
        if (string.IsNullOrWhiteSpace(user))
            throw new ArgumentException("Не задан пользователь RabbitMQ.", nameof(user));

        var settings = new BrokerSettings(
            host.Trim(),
            port,
            user.Trim(),
            password ?? "",
            string.IsNullOrWhiteSpace(vhost) ? "/" : vhost,
            _connectionName);

        lock (_gate)
        {
            CloseCore();
            IBrokerConnection? opened = null;
            try
            {
                opened = _connector.Connect(settings);
                opened.Recovered += ClearPending;
                opened.ConsumerTagChanged += OnConsumerTagChanged;
                var channel = opened.CreateChannel();
                _connection = opened;
                _channel = channel;
            }
            catch
            {
                if (opened != null)
                {
                    opened.Recovered -= ClearPending;
                    opened.ConsumerTagChanged -= OnConsumerTagChanged;
                }

                try
                {
                    opened?.Close();
                }
                catch
                {
                    // Сохраняем исходную ошибку подключения.
                }

                _connection = null;
                _channel = null;
                throw;
            }
        }
    }

    internal void DeclareQueue(string queue) => DeclareQueue(queue, null);

    internal void DeclareQueue(string queue, string? queueType)
    {
        queue = RequireName(queue, "Не задано имя очереди.");
        IReadOnlyDictionary<string, object>? arguments = null;
        if (!string.IsNullOrWhiteSpace(queueType))
        {
            var type = queueType!.Trim().ToLowerInvariant();
            if (type is not ("classic" or "quorum" or "stream"))
                throw new ArgumentException("Тип очереди должен быть classic, quorum или stream.");

            arguments = new Dictionary<string, object> { ["x-queue-type"] = type };
        }

        lock (_gate)
            RequireChannel().DeclareQueue(queue, durable: true, arguments);
    }

    internal void DeclareExisting(string queue)
    {
        queue = RequireName(queue, "Не задано имя очереди.");
        lock (_gate)
            RequireChannel().DeclareExisting(queue);
    }

    internal void Publish(string queue, string? body)
    {
        queue = RequireName(queue, "Не задано имя очереди.");
        PublishTo("", queue, body);
    }

    internal void PublishTo(string? exchange, string routingKey, string? body)
    {
        routingKey = RequireName(routingKey, "Не задан ключ маршрутизации.");
        var payload = _writeEncoding.GetBytes(body ?? "");
        lock (_gate)
            RequireChannel().Publish(exchange?.Trim() ?? "", routingKey, payload, persistent: true);
    }

    internal string GetMessage(string queue)
    {
        queue = RequireName(queue, "Не задано имя очереди.");
        byte[]? payload;
        lock (_gate)
            payload = RequireChannel().TryGetMessage(queue);

        return payload == null || payload.Length == 0
            ? ""
            : _readEncoding.GetString(payload);
    }

    internal string Subscribe(string queue, string? consumerTag)
    {
        queue = RequireName(queue, "Не задано имя очереди.");
        consumerTag = string.IsNullOrWhiteSpace(consumerTag) ? "" : consumerTag!.Trim();
        lock (_gate)
        {
            var channel = RequireChannel();
            if (_consumerTag != null)
                channel.Unsubscribe(_consumerTag);

            channel.SetPrefetch(_prefetch);
            _consumerTag = channel.Subscribe(queue, consumerTag, OnDelivery);
            return _consumerTag;
        }
    }

    internal void Unsubscribe()
    {
        lock (_gate)
        {
            if (_consumerTag == null || _channel == null)
                return;

            _channel.Unsubscribe(_consumerTag);
            _consumerTag = null;
        }
    }

    internal RabbitMessage Receive()
    {
        lock (_inboxGate)
        {
            if (_inbox.Count == 0)
                return new RabbitMessage();

            return ToMessage(_inbox.Dequeue());
        }
    }

    internal int PendingCount()
    {
        lock (_inboxGate)
            return _inbox.Count;
    }

    internal int UnackedCount()
    {
        lock (_inboxGate)
            return _unacked.Count;
    }

    internal void Ack(string? deliveryTag)
    {
        var tag = ParseTag(deliveryTag);
        lock (_gate)
        {
            FindUnacked(tag);
            RequireChannel().Ack(tag);
        }

        Forget(tag);
    }

    internal void Nack(string? deliveryTag, bool requeue)
    {
        var tag = ParseTag(deliveryTag);
        lock (_gate)
        {
            FindUnacked(tag);
            RequireChannel().Nack(tag, requeue);
        }

        Forget(tag);
    }

    internal void Move(string? deliveryTag, string queue)
    {
        queue = RequireName(queue, "Не задано имя очереди.");
        var tag = ParseTag(deliveryTag);
        lock (_gate)
        {
            var pending = FindUnacked(tag);
            var channel = RequireChannel();
            channel.Publish("", queue, pending.Body, persistent: true);
            channel.Ack(tag);
        }

        Forget(tag);
    }

    internal void Disconnect()
    {
        lock (_gate)
            CloseCore();
    }

    private IBrokerChannel RequireChannel()
    {
        if (_channel == null)
            throw new InvalidOperationException("Нет подключения к RabbitMQ. Сначала вызовите Connect.");
        return _channel;
    }

    private void CloseCore()
    {
        if (_connection != null)
        {
            _connection.Recovered -= ClearPending;
            _connection.ConsumerTagChanged -= OnConsumerTagChanged;
        }

        ClearPending();
        _consumerTag = null;
        try
        {
            _channel?.Close();
        }
        finally
        {
            _channel = null;
            try
            {
                _connection?.Close();
            }
            finally
            {
                _connection = null;
            }
        }
    }

    private void OnDelivery(Delivery delivery)
    {
        var pending = new PendingMessage(delivery);
        lock (_inboxGate)
        {
            _inbox.Enqueue(pending);
            _unacked[pending.DeliveryTag] = pending;
        }
    }

    private void OnConsumerTagChanged(string previousTag, string currentTag)
    {
        if (_consumerTag == previousTag)
            _consumerTag = currentTag;
    }

    private void ClearPending()
    {
        lock (_inboxGate)
        {
            _inbox.Clear();
            _unacked.Clear();
        }
    }

    private PendingMessage FindUnacked(ulong tag)
    {
        lock (_inboxGate)
        {
            if (_unacked.TryGetValue(tag, out var pending))
                return pending;
        }

        throw new ArgumentException("Неизвестная метка доставки.");
    }

    private void Forget(ulong tag)
    {
        lock (_inboxGate)
        {
            _unacked.Remove(tag);
            if (_inbox.Count == 0)
                return;

            var kept = new Queue<PendingMessage>(_inbox.Count);
            while (_inbox.Count > 0)
            {
                var pending = _inbox.Dequeue();
                if (pending.DeliveryTag != tag)
                    kept.Enqueue(pending);
            }

            while (kept.Count > 0)
                _inbox.Enqueue(kept.Dequeue());
        }
    }

    private static ulong ParseTag(string? deliveryTag)
    {
        if (string.IsNullOrWhiteSpace(deliveryTag) || !ulong.TryParse(deliveryTag, out var tag))
            throw new ArgumentException("Не задана метка доставки.");
        return tag;
    }

    private RabbitMessage ToMessage(PendingMessage pending) =>
        new(
            _readEncoding.GetString(pending.Body),
            pending.DeliveryTag.ToString(),
            pending.ConsumerTag,
            pending.Exchange,
            pending.RoutingKey,
            pending.Redelivered);

    private sealed class PendingMessage
    {
        public PendingMessage(Delivery delivery)
        {
            DeliveryTag = delivery.DeliveryTag;
            ConsumerTag = delivery.ConsumerTag;
            Exchange = delivery.Exchange;
            RoutingKey = delivery.RoutingKey;
            Redelivered = delivery.Redelivered;
            Body = delivery.Body;
        }

        public ulong DeliveryTag { get; }
        public string ConsumerTag { get; }
        public string Exchange { get; }
        public string RoutingKey { get; }
        public bool Redelivered { get; }
        public byte[] Body { get; }
    }

    private static string RequireName(string? value, string message)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new ArgumentException(message);
        return value!.Trim();
    }
}
