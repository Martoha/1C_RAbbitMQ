using System.Text;
using Moq;
using OneCRabbit;
using RabbitMQ.Client;

namespace OneCRabbit.Tests;

public class RabbitMqConnectorTests
{
    [Fact]
    public void CreateFactory_maps_settings()
    {
        var factory = RabbitMqConnector.CreateFactory(new BrokerSettings("mq", 5673, "app", "secret", "/erp", "billing"));

        Assert.Equal("mq", factory.HostName);
        Assert.Equal(5673, factory.Port);
        Assert.Equal("app", factory.UserName);
        Assert.Equal("secret", factory.Password);
        Assert.Equal("/erp", factory.VirtualHost);
        Assert.Equal("billing", factory.ClientProvidedName);
        Assert.True(factory.AutomaticRecoveryEnabled);
    }

    [Fact]
    public void DeclareQueue_and_publish_use_amqp_channel()
    {
        var properties = new Mock<IBasicProperties>();
        var model = new Mock<IModel>();
        model.Setup(item => item.CreateBasicProperties()).Returns(properties.Object);
        var connection = new Mock<IConnection>();
        connection.Setup(item => item.CreateModel()).Returns(model.Object);

        var client = new RabbitClient(new RabbitMqConnector(_ => connection.Object));
        client.Connect("localhost", 5672, "guest", "guest", "/");
        client.DeclareQueue("orders");
        client.Publish("orders", "hi");

        model.Verify(item => item.QueueDeclare("orders", true, false, false, null));
        model.Verify(item => item.BasicPublish(
            "",
            "orders",
            false,
            properties.Object,
            It.Is<ReadOnlyMemory<byte>>(body => Encoding.UTF8.GetString(body.ToArray()) == "hi")));
        properties.VerifySet(item => item.Persistent = true);
    }

    [Fact]
    public void DeclareExisting_calls_passive_declare()
    {
        var model = new Mock<IModel>();
        var channel = new RabbitChannel(model.Object);

        channel.DeclareExisting("orders");
    }
}
