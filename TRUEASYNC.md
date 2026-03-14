# Адаптация php-amqplib для TrueAsync

## Что такое TrueAsync

TrueAsync — это расширение PHP, которое добавляет конкурентное выполнение через корутины прямо в ядро языка. Главная идея проста и элегантна: **обычный блокирующий PHP-код становится конкурентным автоматически**, без переписывания.

Когда корутина вызывает блокирующую операцию (сокет, файл, DNS), TrueAsync перехватывает её на уровне рантайма, приостанавливает корутину и передаёт управление планировщику. Другие корутины продолжают работать. Когда данные готовы — корутина возобновляется.

```php
use function Async\spawn;
use function Async\await;

$a = spawn(fn() => file_get_contents('http://example.com'));
$b = spawn(fn() => file_get_contents('http://example.org'));

// Оба запроса выполняются конкурентно
await($a);
await($b);
```

Никаких `async/await` в сигнатурах, никаких Promise, никакого переписывания существующего кода.

---

## Главное отличие от Swoole

Для понимания адаптации важно сравнить два подхода:

| | Swoole | TrueAsync |
|---|---|---|
| Режим сокетов | **non-blocking** | **blocking** |
| Механизм ожидания | `Event::add()` + `Coroutine\Channel` | рантайм перехватывает сам |
| Изменений в коде | много | минимум |

В Swoole сокеты должны быть неблокирующими, а библиотека обязана явно использовать Swoole Event API. TrueAsync работает иначе: сокеты остаются **блокирующими**, а весь async происходит прозрачно на уровне C.

Если сокет сделать неблокирующим в TrueAsync — `fread()` будет возвращать пустую строку сразу, и код уйдёт в CPU-bound цикл. Это неправильно.

---

## Что было изменено

Адаптация состоит из **двух новых файлов**. Ни один существующий файл библиотеки не тронут.

### 1. `PhpAmqpLib/Wire/IO/TrueAsyncIO.php`

Новый IO-слой на основе `StreamIO`. Ключевое отличие — одна строка:

```php
// StreamIO (оригинал): зависит от pcntl сигналов
if ($this->canDispatchPcntlSignal) {
    stream_set_blocking($this->sock, false); // non-blocking
} else {
    stream_set_blocking($this->sock, true);
}

// TrueAsyncIO: всегда blocking — TrueAsync перехватит сам
stream_set_blocking($this->sock, true);
```

Метод `read()` упрощён — не нужны `stream_select()` loop и Swoole Event API:

```php
public function read($len)
{
    $this->check_heartbeat();

    $read = 0;
    $data = '';

    while ($read < $len) {
        if (!is_resource($this->sock) || feof($this->sock)) {
            $this->close();
            throw new AMQPConnectionClosedException('Broken pipe or closed connection');
        }

        // fread() блокирует корутину — TrueAsync перехватит и запланирует другие
        $buffer = fread($this->sock, ($len - $read));

        if ($buffer === '') {
            throw new AMQPNoDataException();
        }

        $read += mb_strlen($buffer, 'ASCII');
        $data .= $buffer;
    }

    return $data;
}
```

Метод `do_select()` оставлен с настоящим `stream_select()` — TrueAsync перехватывает и его:

```php
protected function do_select(?int $sec, int $usec)
{
    $read   = [$this->sock];
    $write  = null;
    $except = null;

    return stream_select($read, $write, $except, $sec, $usec);
}
```

Сравните с SwooleIO, где `do_select()` — заглушка:
```php
// SwooleIO — workaround, stream_select не работает в Swoole корутинах
protected function do_select(?int $sec, int $usec)
{
    return true; // просто возвращает true
}
```

### 2. `PhpAmqpLib/Connection/AMQPTrueAsyncConnection.php`

Тонкая обёртка над `AbstractConnection` — создаёт `TrueAsyncIO` вместо `StreamIO`:

```php
class AMQPTrueAsyncConnection extends AbstractConnection
{
    public function __construct(
        string $host,
        int    $port,
        string $user,
        string $password,
        string $vhost               = '/',
        // ... стандартные параметры ...
        array  $ssl_options         = []
    ) {
        $io = new TrueAsyncIO(
            $host, $port,
            $connection_timeout,
            $read_write_timeout,
            $context, $keepalive, $heartbeat
        );

        parent::__construct($user, $password, $vhost, /* ... */, $io, /* ... */);
    }
}
```

Это полная копия паттерна из `AMQPStreamConnection` — только `StreamIO` заменён на `TrueAsyncIO`.

---

## Использование

### Простое подключение

```php
use PhpAmqpLib\Connection\AMQPTrueAsyncConnection;
use PhpAmqpLib\Message\AMQPMessage;

$conn = new AMQPTrueAsyncConnection('localhost', 5672, 'guest', 'guest', '/');
$ch   = $conn->channel();

$ch->queue_declare('my_queue', false, false, false, true);
$ch->basic_publish(new AMQPMessage('Hello!'), '', 'my_queue');

$msg = $ch->basic_get('my_queue');
echo $msg->getBody(); // Hello!

$ch->close();
$conn->close();
```

### Конкурентные корутины

Настоящая сила TrueAsync — два соединения работают **одновременно** в одном потоке:

```php
use function Async\spawn;
use function Async\await;

// Producer и Consumer работают конкурентно
$producer = spawn(function () {
    $conn = new AMQPTrueAsyncConnection('localhost', 5672, 'guest', 'guest', '/');
    $ch   = $conn->channel();
    $ch->queue_declare('demo', false, false, false, true);

    for ($i = 1; $i <= 1000; $i++) {
        $ch->basic_publish(new AMQPMessage("msg #$i"), '', 'demo');
    }

    $ch->close();
    $conn->close();
});

$consumer = spawn(function () {
    $conn = new AMQPTrueAsyncConnection('localhost', 5672, 'guest', 'guest', '/');
    $ch   = $conn->channel();
    $ch->queue_declare('demo', false, false, false, true);

    $received = 0;
    $ch->basic_consume('demo', '', false, true, false, false, function ($msg) use (&$received, $ch) {
        $received++;
        if ($received >= 1000) {
            $ch->basic_cancel($msg->delivery_info['consumer_tag']);
        }
    });

    while (count($ch->callbacks)) {
        $ch->wait(null, false, 10);
    }

    $ch->close();
    $conn->close();
});

await($producer);
await($consumer);
```

Пока producer ждёт подтверждения от RabbitMQ — consumer получает и обрабатывает сообщения. Оба блокируют, но TrueAsync чередует их выполнение автоматически.

---

## Итог

| Что сделано | Сложность |
|---|---|
| Создан `TrueAsyncIO.php` | ~220 строк |
| Создан `AMQPTrueAsyncConnection.php` | ~65 строк |
| Изменено существующих файлов | **0** |

Ключевой принцип адаптации библиотеки под TrueAsync:

> Найди место, где создаётся сокет, и убедись что он в **блокирующем** режиме. Остальное TrueAsync сделает сам.

Репозиторий: [github.com/true-async/php-amqplib](https://github.com/true-async/php-amqplib), ветка `true-async`.
