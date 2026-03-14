<?php
declare(strict_types=1);

namespace PhpAmqpLib\Tests\Functional\TrueAsync;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPTrueAsyncConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Tests\TestCaseCompat;

/**
 * Functional tests for AMQPTrueAsyncConnection.
 *
 * Requires a running RabbitMQ instance (defaults: localhost:5672, guest/guest).
 * Override via env vars: TEST_RABBITMQ_HOST, TEST_RABBITMQ_PORT, TEST_RABBITMQ_USER,
 * TEST_RABBITMQ_PASS.
 *
 * @group connection
 * @group trueasync
 */
class TrueAsyncTest extends TestCaseCompat
{
    protected AbstractConnection $connection;
    protected AMQPChannel $channel;
    protected object $exchange;
    protected object $queue;

    protected function setUpCompat(): void
    {
        $this->connection = new AMQPTrueAsyncConnection(HOST, PORT, USER, PASS, VHOST);
        $this->channel    = $this->connection->channel();

        $this->exchange = (object) ['name' => ''];
        $this->queue    = (object) ['name' => null];
    }

    protected function tearDownCompat(): void
    {
        if (isset($this->channel)) {
            $this->channel->close();
        }
        if (isset($this->connection)) {
            $this->connection->close();
        }
    }

    public function testBasicConsume(): void
    {
        $this->exchange->name = 'test_trueasync_direct_exchange';

        $this->channel->exchange_declare($this->exchange->name, 'direct', false, false, false);

        [$this->queue->name] = $this->channel->queue_declare();

        $this->channel->queue_bind($this->queue->name, $this->exchange->name, $this->queue->name);

        $message = (object) [
            'body'       => 'foo',
            'properties' => [
                'content_type'   => 'text/plain',
                'delivery_mode'  => AMQPMessage::DELIVERY_MODE_NON_PERSISTENT,
                'correlation_id' => 'my_correlation_id',
                'reply_to'       => 'my_reply_to',
            ],
        ];

        $msg = new AMQPMessage($message->body, $message->properties);
        $this->channel->basic_publish($msg, $this->exchange->name, $this->queue->name);

        $callback = function ($msg) use ($message) {
            $this->assertEquals($message->body, $msg->body);
            $this->assertEquals($this->queue->name, $msg->delivery_info['routing_key']);
            $this->assertEquals($this->exchange->name, $msg->delivery_info['exchange']);
            $this->assertFalse($msg->delivery_info['redelivered']);
            $this->assertEquals($message->properties['content_type'], $msg->get('content_type'));
            $this->assertEquals($message->properties['correlation_id'], $msg->get('correlation_id'));
            $this->assertEquals($message->properties['reply_to'], $msg->get('reply_to'));
            $msg->getChannel()->basic_cancel($msg->delivery_info['consumer_tag']);
        };

        $this->channel->basic_consume(
            $this->queue->name,
            '',
            false,
            true,  // no_ack
            false,
            false,
            $callback
        );

        while (count($this->channel->callbacks)) {
            $this->channel->wait(null, false, 5);
        }
    }

    public function testBasicGet(): void
    {
        [$queue] = $this->channel->queue_declare();

        $sent = new AMQPMessage('hello-trueasync-' . mt_rand());
        $this->channel->basic_publish($sent, '', $queue);

        $received = $this->channel->basic_get($queue);

        self::assertInstanceOf(AMQPMessage::class, $received);
        self::assertSame($sent->getBody(), $received->getBody());
        self::assertNotEmpty($received->getDeliveryTag());
        self::assertFalse($received->isRedelivered());

        $received->ack();
    }

    public function testDoubleAckThrowsException(): void
    {
        [$queue] = $this->channel->queue_declare();

        $sent = new AMQPMessage('test-' . mt_rand());
        $this->channel->basic_publish($sent, '', $queue);

        $received = $this->channel->basic_get($queue);

        self::assertSame($sent->getBody(), $received->getBody());
        self::assertNotEmpty($received->getDeliveryTag());
        self::assertSame($this->channel, $received->getChannel());
        self::assertFalse($received->isRedelivered());

        $received->ack();

        $this->expectException(\LogicException::class);
        $received->ack();
    }

    public function testPublishConfirmMode(): void
    {
        [$queue] = $this->channel->queue_declare();

        $confirmed = null;

        $this->channel->set_ack_handler(function (AMQPMessage $message) use (&$confirmed) {
            $confirmed = $message;
        });

        $this->channel->confirm_select();

        $message = new AMQPMessage('test-confirm-' . mt_rand());
        $this->channel->basic_publish($message, '', $queue);

        self::assertGreaterThan(0, $message->getDeliveryTag());

        $this->channel->wait_for_pending_acks(3);

        self::assertSame($message, $confirmed);
    }

    public function testPublishWithConfirmTwoConnections(): void
    {
        $this->exchange->name = 'test_trueasync_topic_exchange';

        $this->channel->exchange_declare($this->exchange->name, 'topic');

        $deliveryTags = [];

        $this->channel->set_ack_handler(function (AMQPMessage $message) use (&$deliveryTags) {
            $deliveryTags[] = (int) $message->get('delivery_tag');
            return false;
        });

        $this->channel->confirm_select();

        $connection2 = new AMQPTrueAsyncConnection(HOST, PORT, USER, PASS, VHOST);
        $channel2    = $connection2->channel();

        $channel2->queue_declare('tst.trueasync.queue');
        $channel2->queue_bind('tst.trueasync.queue', $this->exchange->name, '#');

        $this->channel->basic_publish(new AMQPMessage('foo'), $this->exchange->name);
        $this->channel->basic_publish(new AMQPMessage('bar'), $this->exchange->name);

        $this->channel->wait_for_pending_acks_returns(1);

        $msg1 = $channel2->basic_get('tst.trueasync.queue');
        $msg2 = $channel2->basic_get('tst.trueasync.queue');

        $this->assertInstanceOf(AMQPMessage::class, $msg1);
        $this->assertInstanceOf(AMQPMessage::class, $msg2);
        $this->assertSame('foo', $msg1->getBody());
        $this->assertSame('bar', $msg2->getBody());
        $this->assertSame([1, 2], $deliveryTags);

        $channel2->close();
        $connection2->close();
    }
}
