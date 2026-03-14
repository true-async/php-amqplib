<?php
require __DIR__ . '/vendor/autoload.php';
require __DIR__ . '/tests/config.php';

use PhpAmqpLib\Connection\AMQPTrueAsyncConnection;
use PhpAmqpLib\Message\AMQPMessage;

echo "HOST=" . HOST . " PORT=" . PORT . "\n";

// Simulate what phpunit setUp does - multiple connections
for ($i = 1; $i <= 3; $i++) {
    echo "\n--- Test $i ---\n";
    try {
        $conn = new AMQPTrueAsyncConnection(HOST, PORT, USER, PASS, VHOST);
        echo "Connected OK\n";
        $ch = $conn->channel();
        echo "Channel OK\n";
        [$queue] = $ch->queue_declare('', false, false, true, true);
        $ch->basic_publish(new AMQPMessage("msg $i"), '', $queue);
        $received = $ch->basic_get($queue);
        echo "Received: " . $received->getBody() . "\n";
        $received->ack();
        $ch->close();
        $conn->close();
        echo "Closed OK\n";
    } catch (\Throwable $e) {
        echo "ERROR: " . get_class($e) . ": " . $e->getMessage() . "\n";
        echo $e->getTraceAsString() . "\n";
    }
}

echo "\nDone!\n";
