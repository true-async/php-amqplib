<?php
declare(strict_types=1);

namespace PhpAmqpLib\Connection;

use PhpAmqpLib\Wire\IO\TrueAsyncIO;

/**
 * AMQP connection for TrueAsync.
 *
 * TrueAsync intercepts blocking PHP stream/socket calls at the runtime level,
 * enabling concurrent execution without changing application code.
 * Sockets must be in BLOCKING mode for TrueAsync to work correctly.
 *
 * @see TrueAsyncIO
 */
class AMQPTrueAsyncConnection extends AbstractConnection
{
    public function __construct(
        string $host,
        int    $port,
        string $user,
        string $password,
        string $vhost               = '/',
        bool   $insist              = false,
        string $login_method        = 'AMQPLAIN',
        mixed  $login_response      = null,
        string $locale              = 'en_US',
        float  $connection_timeout  = 3.0,
        float  $read_write_timeout  = 3.0,
        bool   $keepalive           = false,
        int    $heartbeat           = 0,
        float  $channel_rpc_timeout = 0.0,
        // SSL options
        array  $ssl_options         = []
    ) {
        if (!empty($ssl_options)) {

            if (isset($ssl_options['default'])) {
                $ssl_options = [
                    'crypto_method'     => STREAM_CRYPTO_METHOD_TLSv1_2_CLIENT,
                    'verify_peer'       => true,
                    'verify_peer_name'  => true,
                    'allow_self_signed' => true,
                ];
            }

            $context = stream_context_create([
                'ssl' => $ssl_options,
            ]);
        } else {
            $context = null;
        }

        $io = new TrueAsyncIO(
            $host,
            $port,
            $connection_timeout,
            $read_write_timeout,
            $context,
            $keepalive,
            $heartbeat
        );

        parent::__construct(
            $user,
            $password,
            $vhost,
            $insist,
            $login_method,
            $login_response,
            $locale,
            $io,
            $heartbeat,
            $connection_timeout,
            $channel_rpc_timeout
        );
    }
}
