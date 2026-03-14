<?php
require __DIR__ . '/vendor/autoload.php';

use PhpAmqpLib\Connection\AMQPTrueAsyncConnection;
use PhpAmqpLib\Message\AMQPMessage;

echo "Connecting...\n";
$conn = new AMQPTrueAsyncConnection('localhost', 5672, 'guest', 'guest', '/');
echo "Connected OK\n";

$ch = $conn->channel();
echo "Channel OK\n";

// Declare queue
[$queue] = $ch->queue_declare('trueasync_test', false, false, false, true);
echo "Queue: $queue\n";

// Publish
$msg = new AMQPMessage('Hello from TrueAsync!');
$ch->basic_publish($msg, '', $queue);
echo "Published OK\n";

// Get
$received = $ch->basic_get($queue);
echo "Received: " . $received->getBody() . "\n";
$received->ack();

$ch->close();
$conn->close();
echo "Done!\n";
