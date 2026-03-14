<?php
require __DIR__ . '/vendor/autoload.php';

use PhpAmqpLib\Connection\AMQPTrueAsyncConnection;
use PhpAmqpLib\Message\AMQPMessage;
use function Async\spawn;
use function Async\await;

const QUEUE     = 'trueasync_coroutines_test';
const MSG_COUNT = 1000;
const BAR_WIDTH = 30;

function progressBar(string $icon, string $label, int $current, int $total, float $startTime): void
{
    $pct      = $current / $total;
    $filled   = (int) round($pct * BAR_WIDTH);
    $empty    = BAR_WIDTH - $filled;
    $bar      = str_repeat('█', $filled) . str_repeat('░', $empty);
    $elapsed  = microtime(true) - $startTime;
    $rate     = $elapsed > 0 ? (int) round($current / $elapsed) : 0;
    $percent  = str_pad((int) round($pct * 100), 3, ' ', STR_PAD_LEFT) . '%';

    echo "\r$icon  $label  [$bar] $percent  $current/$total  ⚡ {$rate} msg/s   ";
}

$startTime = microtime(true);

echo "🐇 TrueAsync × RabbitMQ — concurrent coroutines demo\n";
echo str_repeat('─', 60) . "\n";

// Producer
$producer = spawn(function () use ($startTime) {
    $conn = new AMQPTrueAsyncConnection('localhost', 5672, 'guest', 'guest', '/');
    $ch   = $conn->channel();
    $ch->queue_declare(QUEUE, false, false, false, true);
    $ch->confirm_select();

    for ($i = 1; $i <= MSG_COUNT; $i++) {
        $ch->basic_publish(new AMQPMessage("message #$i"), '', QUEUE);
        progressBar('📤', 'Sending  ', $i, MSG_COUNT, $startTime);
    }

    $ch->wait_for_pending_acks(5);
    progressBar('📤', 'Sending  ', MSG_COUNT, MSG_COUNT, $startTime);
    echo "\n";

    $ch->close();
    $conn->close();

    return MSG_COUNT;
});

// Consumer
$consumer = spawn(function () use ($startTime) {
    $conn     = new AMQPTrueAsyncConnection('localhost', 5672, 'guest', 'guest', '/');
    $ch       = $conn->channel();
    $ch->queue_declare(QUEUE, false, false, false, true);

    $received = 0;

    $ch->basic_consume(QUEUE, '', false, true, false, false, function ($msg) use (&$received, $ch, $startTime) {
        $received++;
        progressBar('📥', 'Receiving', $received, MSG_COUNT, $startTime);
        if ($received >= MSG_COUNT) {
            echo "\n";
            $ch->basic_cancel($msg->delivery_info['consumer_tag']);
        }
    });

    while (count($ch->callbacks)) {
        $ch->wait(null, false, 10);
    }

    $ch->close();
    $conn->close();

    return $received;
});

await($producer);
$received = await($consumer);

$elapsed = round(microtime(true) - $startTime, 2);
$rps     = round(MSG_COUNT / $elapsed);

echo str_repeat('─', 60) . "\n";
echo "✅  Sent:     " . MSG_COUNT . "\n";
echo "✅  Received: $received\n";
echo "⏱️   Time:     {$elapsed}s\n";
echo "⚡  Speed:    ~{$rps} msg/s\n";
