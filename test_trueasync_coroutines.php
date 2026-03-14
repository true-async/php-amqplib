<?php
require __DIR__ . '/vendor/autoload.php';

use PhpAmqpLib\Connection\AMQPTrueAsyncConnection;
use PhpAmqpLib\Message\AMQPMessage;
use function Async\spawn;
use function Async\await;

const QUEUE     = 'trueasync_coroutines_test';
const MSG_COUNT = 1000;
const BAR_WIDTH = 20;

$progress = ['sent' => 0, 'received' => 0];

function renderProgress(array &$progress, float $startTime): void
{
    $elapsed = microtime(true) - $startTime;

    $makeBar = function (int $current) use ($elapsed): string {
        $pct    = min($current / MSG_COUNT, 1.0);
        $filled = (int) round($pct * BAR_WIDTH);
        $bar    = str_repeat('█', $filled) . str_repeat('░', BAR_WIDTH - $filled);
        $pctStr = str_pad((int) round($pct * 100), 3) . '%';
        $rate   = $elapsed > 0 ? (int) round($current / $elapsed) : 0;
        return "[$bar] $pctStr " . str_pad((string)$current, 4) . '/' . MSG_COUNT . " ⚡{$rate}";
    };

    echo "\r📤 " . $makeBar($progress['sent']) . "  │  📥 " . $makeBar($progress['received']) . " msg/s   ";
}

$startTime = microtime(true);

echo "🐇 TrueAsync × RabbitMQ — concurrent coroutines\n";
echo str_repeat('─', 75) . "\n";

$producer = spawn(function () use (&$progress, $startTime) {
    $conn = new AMQPTrueAsyncConnection('localhost', 5672, 'guest', 'guest', '/');
    $ch   = $conn->channel();
    $ch->queue_declare(QUEUE, false, false, false, true);
    $ch->confirm_select();

    for ($i = 1; $i <= MSG_COUNT; $i++) {
        $ch->basic_publish(new AMQPMessage("message #$i"), '', QUEUE);
        Async\delay(rand(1, 50));
        $progress['sent'] = $i;
        renderProgress($progress, $startTime);
    }

    $ch->wait_for_pending_acks(5);
    $ch->close();
    $conn->close();

    return MSG_COUNT;
});

$consumer = spawn(function () use (&$progress, $startTime) {
    $conn = new AMQPTrueAsyncConnection('localhost', 5672, 'guest', 'guest', '/');
    $ch   = $conn->channel();
    $ch->queue_declare(QUEUE, false, false, false, true);

    $received = 0;

    $ch->basic_consume(QUEUE, '', false, true, false, false, function ($msg) use (&$received, &$progress, $ch, $startTime) {
        $received++;
        $progress['received'] = $received;
        Async\delay(rand(1, 50));
        renderProgress($progress, $startTime);
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

echo str_repeat('─', 75) . "\n";
echo "✅  Sent:     " . MSG_COUNT . "\n";
echo "✅  Received: $received\n";
echo "⏱️   Time:     {$elapsed}s\n";
echo "⚡  Speed:    ~{$rps} msg/s\n";
