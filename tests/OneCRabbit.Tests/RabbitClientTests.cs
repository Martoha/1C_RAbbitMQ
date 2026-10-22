using System.Text;
using OneCRabbit;

namespace OneCRabbit.Tests;

public class RabbitClientTests
{
    [Fact]
    public void Connect_rejects_empty_host()
    {
        var client = Client();
        var error = Assert.Throws<ArgumentException>(() => client.Connect("  ", 5672, "guest", "guest", "/"));
        Assert.StartsWith("Не задан адрес сервера RabbitMQ.", error.Message);
    }

    [Fact]
    public void Connect_rejects_port_out_of_range()
    {
        var client = Client();
        var error = Assert.Throws<ArgumentOutOfRangeException>(() => client.Connect("localhost", 0, "guest", "guest", "/"));
        Assert.StartsWith("Порт должен быть от 1 до 65535.", error.Message);
    }

    [Fact]
    public void Connect_rejects_empty_user()
    {
        var client = Client();
        var error = Assert.Throws<ArgumentException>(() => client.Connect("localhost", 5672, "", "guest", "/"));
        Assert.StartsWith("Не задан пользователь RabbitMQ.", error.Message);
    }

    [Fact]
    public void Connect_uses_root_vhost_when_empty()
    {
        var connection = new FakeConnection();
        var connector = new FakeConnector(connection);
        var client = new RabbitClient(connector);

        client.Connect("localhost", 5672, "guest", "guest", "  ");

        Assert.Equal("/", connector.LastSettings!.VirtualHost);
        Assert.True(client.IsConnected());
    }

    [Fact]
    public void Connect_passes_settings_to_broker()
    {
        var connector = new FakeConnector(new FakeConnection());
        var client = new RabbitClient(connector);

        client.Connect(" mq.local ", 5673, "app", "secret", "/erp");

        var settings = connector.LastSettings!;
        Assert.Equal("mq.local", settings.Host);
        Assert.Equal(5673, settings.Port);
        Assert.Equal("app", settings.User);
        Assert.Equal("secret", settings.Password);
        Assert.Equal("/erp", settings.VirtualHost);
    }

    [Fact]
    public void Connect_failure_leaves_client_disconnected()
    {
        var client = new RabbitClient(new ThrowingConnector());

        var error = Assert.Throws<InvalidOperationException>(() =>
            client.Connect("localhost", 5672, "guest", "guest", "/"));

        Assert.Equal("broker down", error.Message);
        Assert.False(client.IsConnected());
    }

    [Fact]
    public void Second_connect_closes_previous_connection()
    {
        var first = new FakeConnection();
        var second = new FakeConnection();
        var client = new RabbitClient(new FakeConnector(first, second));

        client.Connect("localhost", 5672, "guest", "guest", "/");
        client.Connect("other", 5672, "guest", "guest", "/");

        Assert.Equal(1, first.Channel.CloseCount);
        Assert.Equal(1, first.CloseCount);
        Assert.Equal(0, second.CloseCount);
        Assert.True(client.IsConnected());
    }

    [Fact]
    public void Commands_require_connection()
    {
        var client = Client();

        Assert.StartsWith("Нет подключения к RabbitMQ. Сначала вызовите Connect.",
            Assert.Throws<InvalidOperationException>(() => client.DeclareQueue("orders")).Message);
        Assert.StartsWith("Нет подключения к RabbitMQ. Сначала вызовите Connect.",
            Assert.Throws<InvalidOperationException>(() => client.Publish("orders", "hi")).Message);
        Assert.StartsWith("Нет подключения к RabbitMQ. Сначала вызовите Connect.",
            Assert.Throws<InvalidOperationException>(() => client.GetMessage("orders")).Message);
    }

    [Fact]
    public void DeclareQueue_rejects_empty_name()
    {
        var client = ConnectedClient(out _);
        var error = Assert.Throws<ArgumentException>(() => client.DeclareQueue(" "));
        Assert.StartsWith("Не задано имя очереди.", error.Message);
    }

    [Fact]
    public void DeclareQueue_creates_durable_queue()
    {
        var client = ConnectedClient(out var connection);
        client.DeclareQueue("orders");
        Assert.Equal(("orders", true, null), connection.Channel.Queues.Single());
    }

    [Fact]
    public void DeclareQueueType_marks_quorum_queue()
    {
        var client = ConnectedClient(out var connection);
        client.DeclareQueueType("test", "Quorum");
        Assert.Equal(("test", true, "quorum"), connection.Channel.Queues.Single());
    }

    [Fact]
    public void Publish_writes_windows1251_and_receive_reads_it_as_text()
    {
        var client = ConnectedClient(out var connection);
        client.SetWriteEncoding("win1251");
        client.SetReadEncoding("cp1251");
        client.Subscribe("orders", "tag");

        client.Publish("orders", "СЛАТА");
        var payload = connection.Channel.Published.Single().Body;
        Assert.Equal(Encoding.GetEncoding(1251).GetBytes("СЛАТА"), payload);

        connection.Channel.Deliver(1, payload);
        Assert.Equal("СЛАТА", client.Receive().Body);
    }

    [Fact]
    public void Headers_are_sent_and_read_back()
    {
        var client = ConnectedClient(out var connection);
        client.SetHeader("sign", "abc");
        client.SetHeader("source", "1c");
        client.Subscribe("orders", "tag");
        client.Publish("orders", "тело");

        var headers = connection.Channel.Published.Single().Headers;
        Assert.NotNull(headers);
        Assert.Equal("abc", headers!["sign"]);
        Assert.Equal("1c", headers["source"]);

        connection.Channel.Deliver(4, Encoding.UTF8.GetBytes("тело"), headers: headers);
        var message = client.Receive();
        Assert.Equal("abc", message.Header("sign"));
        Assert.Equal("1c", message.Header("source"));
        Assert.Equal("", message.Header("missing"));
    }

    [Fact]
    public void DeclareExisting_checks_queue_without_recreating_it()
    {
        var client = ConnectedClient(out var connection);
        client.DeclareExisting("orders");
        Assert.Equal("orders", connection.Channel.Existing.Single());
        Assert.Empty(connection.Channel.Queues);
    }

    [Fact]
    public void Publish_sends_persistent_utf8_to_default_exchange()
    {
        var client = ConnectedClient(out var connection);
        client.Publish("orders", "Привет");

        var message = connection.Channel.Published.Single();
        Assert.Equal("", message.Exchange);
        Assert.Equal("orders", message.RoutingKey);
        Assert.Equal("Привет", Encoding.UTF8.GetString(message.Body));
        Assert.True(message.Persistent);
    }

    [Fact]
    public void Publish_treats_null_body_as_empty()
    {
        var client = ConnectedClient(out var connection);
        client.Publish("orders", null!);
        Assert.Empty(connection.Channel.Published.Single().Body);
    }

    [Fact]
    public void Publish_rejects_empty_queue()
    {
        var client = ConnectedClient(out _);
        var error = Assert.Throws<ArgumentException>(() => client.Publish("", "hi"));
        Assert.StartsWith("Не задано имя очереди.", error.Message);
    }

    [Fact]
    public void PublishTo_allows_empty_exchange()
    {
        var client = ConnectedClient(out var connection);
        client.PublishTo("", "orders", "body");
        Assert.Equal("", connection.Channel.Published.Single().Exchange);
    }

    [Fact]
    public void PublishTo_rejects_empty_routing_key()
    {
        var client = ConnectedClient(out _);
        var error = Assert.Throws<ArgumentException>(() => client.PublishTo("amq.direct", " ", "body"));
        Assert.StartsWith("Не задан ключ маршрутизации.", error.Message);
    }

    [Fact]
    public void GetMessage_returns_empty_string_when_queue_is_empty()
    {
        var client = ConnectedClient(out _);
        Assert.Equal("", client.GetMessage("orders"));
    }

    [Fact]
    public void GetMessage_decodes_utf8()
    {
        var client = ConnectedClient(out var connection);
        connection.Channel.Incoming.Enqueue(Encoding.UTF8.GetBytes("заказ"));
        Assert.Equal("заказ", client.GetMessage("orders"));
    }

    [Fact]
    public void GetMessage_rejects_empty_queue()
    {
        var client = ConnectedClient(out _);
        var error = Assert.Throws<ArgumentException>(() => client.GetMessage(""));
        Assert.StartsWith("Не задано имя очереди.", error.Message);
    }

    [Fact]
    public void Disconnect_without_connection_is_safe()
    {
        var client = Client();
        client.Disconnect();
        Assert.False(client.IsConnected());
    }

    [Fact]
    public void Disconnect_closes_channel_and_connection()
    {
        var client = ConnectedClient(out var connection);
        client.Disconnect();
        Assert.Equal(1, connection.Channel.CloseCount);
        Assert.Equal(1, connection.CloseCount);
        Assert.False(client.IsConnected());
    }

    private static RabbitClient Client() => new(new FakeConnector());

    private static RabbitClient ConnectedClient(out FakeConnection connection)
    {
        connection = new FakeConnection();
        var client = new RabbitClient(new FakeConnector(connection));
        client.Connect("localhost", 5672, "guest", "guest", "/");
        return client;
    }
}
