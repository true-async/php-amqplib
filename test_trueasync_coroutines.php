<?php
require __DIR__ . '/vendor/autoload.php';

use PhpAmqpLib\Connection\AMQPTrueAsyncConnection;
use PhpAmqpLib\Message\AMQPMessage;
use function Async\spawn;
use function Async\await;

const QUEUE     = 'trueasync_coroutines_test';
const MSG_COUNT = 1000;

$startTime = microtime(true);

// Producer: sends MSG_COUNT messages
$producer = spawn(function () {
    $conn = new AMQPTrueAsyncConnection('localhost', 5672, 'guest', 'guest', '/');
    $ch   = $conn->channel();
    $ch->queue_declare(QUEUE, false, false, false, true);
    $ch->confirm_select();

    $sent = 0;
    for ($i = 1; $i <= MSG_COUNT; $i++) {
        $ch->basic_publish(new AMQPMessage("message #$i"), '', QUEUE);
        $sent++;
        if ($sent % 100 === 0) {
            echo "\r[producer] sent:     $sent / " . MSG_COUNT;
        }
    }

    $ch->wait_for_pending_acks(5);
    echo "\r[producer] sent:     $sent / " . MSG_COUNT . " — done!       \n";

    $ch->close();
    $conn->close();

    return $sent;
});

// Consumer: receives MSG_COUNT messages
$consumer = spawn(function () {
    $conn     = new AMQPTrueAsyncConnection('localhost', 5672, 'guest', 'guest', '/');
    $ch       = $conn->channel();
    $ch->queue_declare(QUEUE, false, false, false, true);

    $received = 0;

    $ch->basic_consume(QUEUE, '', false, true, false, false, function ($msg) use (&$received, $ch) {
        $received++;
        if ($received % 100 === 0) {
            echo "\r[consumer] received: $received / " . MSG_COUNT;
        }
        if ($received >= MSG_COUNT) {
            echo "\r[consumer] received: $received / " . MSG_COUNT . " — done!       \n";
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

$sent     = await($producer);
$received = await($consumer);

$elapsed = round(microtime(true) - $startTime, 2);
$rps     = round(MSG_COUNT / $elapsed);

echo "\n";
echo "Sent:     $sent\n";
echo "Received: $received\n";
echo "Time:     {$elapsed}s\n";
echo "Speed:    ~{$rps} msg/s\n";
