using System.Text;
using OneCRabbit;

namespace OneCRabbit.Tests;

public class SubscriptionTests
{
    [Fact]
    public void Connect_uses_default_connection_name()
    {
        var connector = new FakeConnector(new FakeConnection());
        var client = new RabbitClient(connector);

        client.Connect("localhost", 5672, "guest", "guest", "/");

        Assert.Equal("1C", connector.LastSettings!.ClientName);
    }

    [Fact]
    public void Connect_uses_connection_name_set_before_connect()
    {
        var connector = new FakeConnector(new FakeConnection());
        var client = new RabbitClient(connector);

        client.SetConnectionName(" 1C-orders ");
        client.Connect("localhost", 5672, "guest", "guest", "/");

        Assert.Equal("1C-orders", connector.LastSettings!.ClientName);
    }

    [Fact]
    public void Subscribe_buffers_every_message_without_ack()
    {
        var client = Subscribed(out var channel);
        channel.Deliver(10, Encoding.UTF8.GetBytes("первое"));
        channel.Deliver(11, Encoding.UTF8.GetBytes("второе"));

        var first = client.Receive();
        var second = client.Receive();
        var empty = client.Receive();

        Assert.True(first.HasMessage);
        Assert.Equal("первое", first.Body);
        Assert.Equal("10", first.DeliveryTag);
        Assert.Equal("orders-1c", first.ConsumerTag);
        Assert.Equal("второе", second.Body);
        Assert.False(empty.HasMessage);
        Assert.Empty(channel.Acks);
        Assert.Equal(0, client.PendingCount());
        Assert.Equal(2, client.UnackedCount());
    }

    [Fact]
    public void Subscribe_requests_unlimited_prefetch_by_default()
    {
        Subscribed(out var channel);
        Assert.Equal((ushort)0, channel.Prefetch);
    }

    [Fact]
    public void Subscribe_uses_requested_prefetch_and_consumer_tag()
    {
        var client = Connected(out var channel);
        client.SetPrefetch(25);
        var tag = client.Subscribe("orders", "billing");

        Assert.Equal("billing", tag);
        Assert.Equal((ushort)25, channel.Prefetch);
        Assert.Equal("billing", channel.ActiveConsumerTag);
    }

    [Fact]
    public void Subscribe_with_empty_tag_keeps_server_tag()
    {
        var client = Connected(out _);
        Assert.Equal("amq.ctag-server", client.Subscribe("orders", "  "));
    }

    [Fact]
    public void Ack_confirms_one_message()
    {
        var client = Subscribed(out var channel);
        channel.Deliver(10, Encoding.UTF8.GetBytes("a"));
        channel.Deliver(11, Encoding.UTF8.GetBytes("b"));
        client.Receive();

        client.Ack("10");

        Assert.Equal(new[] { 10ul }, channel.Acks);
        Assert.Equal(1, client.UnackedCount());
    }

    [Fact]
    public void Nack_can_requeue_message()
    {
        var client = Subscribed(out var channel);
        channel.Deliver(11, Encoding.UTF8.GetBytes("b"));
        client.Receive();

        client.Nack("11", true);

        Assert.Equal((11ul, true), channel.Nacks.Single());
        Assert.Equal(0, client.UnackedCount());
    }

    [Fact]
    public void Move_republishes_original_bytes_and_acks()
    {
        var client = Subscribed(out var channel);
        var body = Encoding.UTF8.GetBytes("заказ");
        channel.Deliver(7, body);
        client.Receive();

        client.Move("7", "archive");

        var published = channel.Published.Single();
        Assert.Equal("", published.Exchange);
        Assert.Equal("archive", published.RoutingKey);
        Assert.Equal(body, published.Body);
        Assert.True(published.Persistent);
        Assert.Equal(new[] { 7ul }, channel.Acks);
        Assert.Equal(0, client.UnackedCount());
    }

    [Fact]
    public void Ack_rejects_unknown_tag()
    {
        var client = Subscribed(out var channel);
        channel.Deliver(10, Encoding.UTF8.GetBytes("a"));

        var error = Assert.Throws<ArgumentException>(() => client.Ack("99"));

        Assert.StartsWith("Неизвестная метка доставки.", error.Message);
        Assert.Empty(channel.Acks);
    }

    [Fact]
    public void Recovery_drops_old_delivery_tags()
    {
        var connection = new FakeConnection();
        var client = new RabbitClient(new FakeConnector(connection));
        client.Connect("localhost", 5672, "guest", "guest", "/");
        client.Subscribe("orders", "orders-1c");
        connection.Channel.Deliver(10, Encoding.UTF8.GetBytes("a"));

        connection.RaiseRecovered();

        Assert.Equal(0, client.PendingCount());
        Assert.Equal(0, client.UnackedCount());
        Assert.False(client.Receive().HasMessage);
        Assert.Throws<ArgumentException>(() => client.Ack("10"));
    }

    [Fact]
    public void Unsubscribe_cancels_consumer_and_keeps_unacked_messages()
    {
        var client = Subscribed(out var channel);
        channel.Deliver(10, Encoding.UTF8.GetBytes("a"));
        client.Receive();

        client.Unsubscribe();

        Assert.Equal("orders-1c", channel.Cancelled.Single());
        Assert.Equal(1, client.UnackedCount());
    }

    [Fact]
    public void SetPrefetch_rejects_value_outside_ushort()
    {
        var client = Connected(out _);
        Assert.Throws<ArgumentOutOfRangeException>(() => client.SetPrefetch(-1));
    }

    private static RabbitClient Connected(out FakeChannel channel)
    {
        var connection = new FakeConnection();
        channel = connection.Channel;
        var client = new RabbitClient(new FakeConnector(connection));
        client.Connect("localhost", 5672, "guest", "guest", "/");
        return client;
    }

    private static RabbitClient Subscribed(out FakeChannel channel)
    {
        var client = Connected(out channel);
        client.Subscribe("orders", "orders-1c");
        return client;
    }
}
