using System.Reflection;
using OneCRabbit;

var console = new DebugConsole();
console.Run();

internal sealed class DebugConsole
{
    private readonly RabbitClient _client = new();

    public void Run()
    {
        Console.CancelKeyPress += (_, eventArgs) =>
        {
            eventArgs.Cancel = true;
            Disconnect();
            Environment.Exit(0);
        };

        PrintHelp();
        while (true)
        {
            Console.Write("> ");
            var line = Console.ReadLine();
            if (line is null)
                break;

            if (string.IsNullOrWhiteSpace(line))
                continue;

            try
            {
                if (!Execute(line.Trim()))
                    break;
            }
            catch (Exception exception)
            {
                Console.WriteLine("Ошибка: " + Unwrap(exception).Message);
            }
        }

        Disconnect();
    }

    private bool Execute(string line)
    {
        var parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var command = parts[0].ToLowerInvariant();
        var rest = parts.Skip(1).ToArray();

        switch (command)
        {
            case "help":
            case "?":
                PrintHelp();
                return true;
            case "name":
                SetConnectionName(rest);
                return true;
            case "connect":
                Connect(rest);
                return true;
            case "status":
                Status();
                return true;
            case "queue":
                DeclareQueue(rest);
                return true;
            case "use":
                UseQueue(rest);
                return true;
            case "publish":
                Publish(line);
                return true;
            case "publish-to":
                PublishTo(line);
                return true;
            case "get":
                GetMessage(rest);
                return true;
            case "prefetch":
                SetPrefetch(rest);
                return true;
            case "read-encoding":
                SetReadEncoding(rest);
                return true;
            case "write-encoding":
                SetWriteEncoding(rest);
                return true;
            case "header":
                SetHeader(line);
                return true;
            case "clear-headers":
                _client.ClearHeaders();
                Console.WriteLine("Атрибуты сообщения очищены");
                return true;
            case "subscribe":
                Subscribe(rest);
                return true;
            case "unsubscribe":
                _client.Unsubscribe();
                Console.WriteLine("Подписка снята");
                return true;
            case "receive":
                Receive();
                return true;
            case "ack":
                Ack(rest);
                return true;
            case "nack":
                Nack(rest);
                return true;
            case "move":
                Move(rest);
                return true;
            case "disconnect":
                Disconnect();
                return true;
            case "exit":
            case "quit":
                return false;
            default:
                Console.WriteLine("Неизвестная команда. help — список.");
                return true;
        }
    }

    private void SetConnectionName(string[] rest)
    {
        Require(rest, 1, "name <имя>");
        _client.SetConnectionName(rest[0]);
        Console.WriteLine("Имя подключения будет применено при следующем connect: " + rest[0]);
    }

    private void Connect(string[] rest)
    {
        var host = Value(rest, 0, "localhost");
        var port = int.Parse(Value(rest, 1, "5672"));
        var user = Value(rest, 2, "guest");
        var password = Value(rest, 3, "guest");
        var vhost = Value(rest, 4, "/");
        _client.Connect(host, port, user, password, vhost);
        Console.WriteLine($"Подключено {host}:{port}, пользователь={user}, vhost={vhost}");
        Status();
    }

    private void Status()
    {
        Console.WriteLine(
            $"IsConnected={_client.IsConnected()}, чтение={_client.ReadEncoding()}, запись={_client.WriteEncoding()}, в буфере={_client.PendingCount()}, неподтверждено={_client.UnackedCount()}");
    }

    private void DeclareQueue(string[] rest)
    {
        Require(rest, 1, "queue <очередь> [classic|quorum|stream]");
        if (rest.Length > 1)
            _client.DeclareQueueType(rest[0], rest[1]);
        else
            _client.DeclareQueue(rest[0]);

        Console.WriteLine(rest.Length > 1
            ? $"Очередь объявлена: {rest[0]}, тип={rest[1]}"
            : "Очередь объявлена: " + rest[0]);
    }

    private void UseQueue(string[] rest)
    {
        Require(rest, 1, "use <очередь>");
        _client.DeclareExisting(rest[0]);
        Console.WriteLine("Очередь уже существует, аргументы не менялись: " + rest[0]);
    }

    private void Publish(string line)
    {
        var (queue, body) = SplitTwo(line, "publish <очередь> <текст>");
        _client.Publish(queue, body);
        Console.WriteLine($"Опубликовано в {queue}: {body}");
    }

    private void PublishTo(string line)
    {
        var parts = line.Split(' ', 4, StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 4)
            throw new ArgumentException("Формат: publish-to <обменник> <ключ> <текст>");

        _client.PublishTo(parts[1], parts[2], parts[3]);
        Console.WriteLine($"Опубликовано в обменник {parts[1]}, ключ {parts[2]}: {parts[3]}");
    }

    private void GetMessage(string[] rest)
    {
        Require(rest, 1, "get <очередь>");
        var body = _client.GetMessage(rest[0]);
        Console.WriteLine(string.IsNullOrEmpty(body)
            ? "Очередь пуста. Сообщение сразу подтверждено, если бы оно было."
            : "GetMessage забрал и сразу подтвердил: " + body);
    }

    private void SetPrefetch(string[] rest)
    {
        Require(rest, 1, "prefetch <число>");
        var count = int.Parse(rest[0]);
        _client.SetPrefetch(count);
        Console.WriteLine(count == 0
            ? "Prefetch = 0, брокер отдаст все сообщения"
            : "Prefetch = " + count);
    }

    private void SetReadEncoding(string[] rest)
    {
        Require(rest, 1, "read-encoding <utf-8|windows-1251>");
        _client.SetReadEncoding(rest[0]);
        Console.WriteLine("Чтение из очереди: " + _client.ReadEncoding());
    }

    private void SetWriteEncoding(string[] rest)
    {
        Require(rest, 1, "write-encoding <utf-8|windows-1251>");
        _client.SetWriteEncoding(rest[0]);
        Console.WriteLine("Запись в очередь: " + _client.WriteEncoding());
    }

    private void SetHeader(string line)
    {
        var parts = line.Split(' ', 3, StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 3)
            throw new ArgumentException("Формат: header <имя> <значение>");

        _client.SetHeader(parts[1], parts[2]);
        Console.WriteLine($"Атрибут {parts[1]}={parts[2]}");
    }

    private void Subscribe(string[] rest)
    {
        Require(rest, 1, "subscribe <очередь> [тег]");
        var tag = _client.Subscribe(rest[0], rest.Length > 1 ? rest[1] : "");
        Console.WriteLine($"Подписка на {rest[0]}, потребитель={tag}");
    }

    private void Receive()
    {
        var message = _client.Receive();
        if (!message.HasMessage)
        {
            Console.WriteLine("В буфере пусто");
            Status();
            return;
        }

        Console.WriteLine("Текст: " + message.Body);
        Console.WriteLine("Метка: " + message.DeliveryTag);
        Console.WriteLine("Потребитель: " + message.ConsumerTag);
        Console.WriteLine("Обменник: " + message.Exchange);
        Console.WriteLine("Ключ: " + message.RoutingKey);
        Console.WriteLine("Повторная доставка: " + (message.Redelivered ? "да" : "нет"));
        foreach (var name in message.HeaderNames().Split(", ", StringSplitOptions.RemoveEmptyEntries))
            Console.WriteLine($"Атрибут {name}: {message.Header(name)}");
        Status();
    }

    private void Ack(string[] rest)
    {
        Require(rest, 1, "ack <метка>");
        _client.Ack(rest[0]);
        Console.WriteLine("Подтверждено: " + rest[0]);
        Status();
    }

    private void Nack(string[] rest)
    {
        Require(rest, 2, "nack <метка> <да|нет>");
        var requeue = ParseBool(rest[1]);
        _client.Nack(rest[0], requeue);
        Console.WriteLine(requeue
            ? "Возвращено в ту же очередь: " + rest[0]
            : "Снято без возврата: " + rest[0]);
        Status();
    }

    private void Move(string[] rest)
    {
        Require(rest, 2, "move <метка> <очередь>");
        _client.Move(rest[0], rest[1]);
        Console.WriteLine($"Переложено {rest[0]} в очередь {rest[1]}");
        Status();
    }

    private void Disconnect()
    {
        if (!_client.IsConnected())
        {
            Console.WriteLine("Соединения нет");
            return;
        }

        _client.Disconnect();
        Console.WriteLine("Соединение закрыто");
    }

    private static Exception Unwrap(Exception exception)
    {
        while (exception.InnerException != null
            && exception is TypeInitializationException or TargetInvocationException)
            exception = exception.InnerException;

        return exception;
    }

    private static void PrintHelp()
    {
        Console.WriteLine("""
            Ручная консоль тех же методов, что вызывает 1С.
            Точку останова ставьте на нужную команду и запускайте этот проект отладчиком.

            name <имя>                          SetConnectionName, до connect
            connect [хост] [порт] [пользователь] [пароль] [vhost]
            status
            queue <очередь> [quorum]             создать очередь
            use <очередь>                       существующая очередь, без смены аргументов
            publish <очередь> <текст>           Publish
            publish-to <обменник> <ключ> <текст>
            get <очередь>                       GetMessage, сразу подтверждает
            prefetch <число>                    0 — все сообщения, максимум 65535
            read-encoding <utf-8|windows-1251>  как раскодировать тело из очереди
            write-encoding <utf-8|windows-1251> как закодировать тело при отправке
            header <имя> <значение>             атрибут сообщения, например sign
            clear-headers                       убрать атрибуты следующих Publish
            subscribe <очередь> [тег]           Subscribe
            receive                             Receive, без подтверждения
            ack <метка>
            nack <метка> <да|нет>               да — вернуть в ту же очередь
            move <метка> <очередь>
            unsubscribe
            disconnect
            help
            exit
            """);
    }

    private static (string Queue, string Body) SplitTwo(string line, string format)
    {
        var parts = line.Split(' ', 3, StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 3)
            throw new ArgumentException("Формат: " + format);

        return (parts[1], parts[2]);
    }

    private static void Require(string[] rest, int count, string format)
    {
        if (rest.Length < count)
            throw new ArgumentException("Формат: " + format);
    }

    private static string Value(string[] rest, int index, string fallback) =>
        rest.Length > index ? rest[index] : fallback;

    private static bool ParseBool(string value) => value.ToLowerInvariant() switch
    {
        "да" or "true" or "1" or "yes" => true,
        "нет" or "false" or "0" or "no" => false,
        _ => throw new ArgumentException("Ожидалось да или нет, получено: " + value)
    };
}
