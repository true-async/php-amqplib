<?php
declare(strict_types=1);

namespace PhpAmqpLib\Wire\IO;

use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Exception\AMQPDataReadException;
use PhpAmqpLib\Exception\AMQPIOException;
use PhpAmqpLib\Exception\AMQPNoDataException;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Helper\MiscHelper;
use PhpAmqpLib\Helper\SocketConstants;
use function is_resource;
use function get_resource_type;
use function stream_context_create;
use function stream_context_set_option;
use function stream_context_get_options;
use function stream_socket_client;
use function stream_socket_get_name;
use function stream_set_timeout;
use function stream_set_blocking;
use function socket_import_stream;
use function socket_set_option;
use function stream_socket_enable_crypto;
use function socket_strerror;
use function feof;
use function fread;
use function fwrite;
use function fclose;
use function microtime;
use function mb_strlen;
use function preg_match;
use function str_starts_with;
use function time;
use const STREAM_CLIENT_CONNECT;
use const STREAM_CRYPTO_METHOD_ANY_CLIENT;
use const SOL_SOCKET;
use const SO_KEEPALIVE;

/**
 * TrueAsync-compatible IO implementation.
 *
 * TrueAsync intercepts blocking PHP socket/stream calls at the runtime level,
 * making them concurrent automatically. Therefore, sockets must remain in
 * BLOCKING mode — this is the only requirement for TrueAsync compatibility.
 *
 * Compare with SwooleIO which requires non-blocking mode + Event::add/del + Channel.
 */
class TrueAsyncIO extends AbstractIO
{
    /** @var null|resource */
    protected $context;

    /** @var null|resource */
    private mixed $sock = null;

    public function __construct(
        string $host,
        int    $port,
        float  $connectionTimeout,
        float  $readWriteTimeout,
        mixed  $context    = null,
        bool   $keepalive  = false,
        int    $heartbeat  = 0
    ) {
        if ($heartbeat !== 0 && ($readWriteTimeout < ($heartbeat * 2))) {
            throw new \InvalidArgumentException('read_write_timeout must be at least 2x the heartbeat');
        }

        if (!is_resource($context) || get_resource_type($context) !== 'stream-context') {
            $context = stream_context_create();
        }

        $this->host               = $host;
        $this->port               = $port;
        $this->connection_timeout = $connectionTimeout;
        $this->read_timeout       = $readWriteTimeout;
        $this->write_timeout      = $readWriteTimeout;
        $this->context            = $context;
        $this->keepalive          = $keepalive;
        $this->heartbeat          = $heartbeat;
        $this->initial_heartbeat  = $heartbeat;

        stream_context_set_option($this->context, 'socket', 'tcp_nodelay', true);

        $options = stream_context_get_options($this->context);

        if (!empty($options['ssl'])
            && !isset($options['ssl']['crypto_method'])
            && !stream_context_set_option($this->context, 'ssl', 'crypto_method', STREAM_CRYPTO_METHOD_ANY_CLIENT)) {
            throw new AMQPIOException("Can not set ssl.crypto_method stream context option");
        }
    }

    public function connect(): void
    {
        $this->setErrorHandler();

        try {
            $this->tryConnect();
        } catch (AMQPIOException $exception) {
            throw $exception;
        } catch (\Throwable $exception) {
            throw new AMQPIOException($exception->getMessage(), 0, $exception);
        } finally {
            $this->restoreErrorHandler();
        }
    }

    private function tryConnect(): void
    {
        $error  = $errno = null;
        $remote = sprintf('tcp://%s:%s', $this->host, $this->port);

        try {
            $this->sock = stream_socket_client(
                $remote,
                $errno,
                $error,
                $this->connection_timeout,
                STREAM_CLIENT_CONNECT,
                $this->context
            );

            $this->throwOnError();
        } catch (\Throwable $exception) {
            $this->sock = false;
            throw new AMQPIOException($exception->getMessage(), 0, $exception);
        } finally {
            $this->restoreErrorHandler();
        }

        if (false === $this->sock) {
            throw new AMQPIOException(
                sprintf('Error Connecting to server(%s): %s ', $errno, $error), $errno ?? 0
            );
        }

        if (!stream_socket_get_name($this->sock, true)) {
            throw new AMQPIOException(sprintf('Connection refused: %s ', $remote));
        }

        [$sec, $uSec] = MiscHelper::splitSecondsMicroseconds(max($this->read_timeout, $this->write_timeout));

        if (!stream_set_timeout($this->sock, $sec, $uSec)) {
            throw new AMQPIOException('Timeout could not be set');
        }

        // TrueAsync requires sockets in BLOCKING mode.
        // TrueAsync intercepts blocking calls at the runtime level and makes them concurrent automatically.
        stream_set_blocking($this->sock, true);

        if ($this->keepalive) {
            $socket = socket_import_stream($this->sock);
            socket_set_option($socket, SOL_SOCKET, SO_KEEPALIVE, 1);
        }

        $options = stream_context_get_options($this->context);

        if (isset($options['ssl']['crypto_method'])) {
            $this->enableCrypto();
        }

        $this->heartbeat = $this->initial_heartbeat;
    }

    private function enableCrypto(): void
    {
        $timeout_at = time() + ($this->read_timeout + $this->write_timeout) * 2;

        set_error_handler(static function ($code, $message) {
            throw new AMQPIOException('Can not enable crypto: ' . $message, $code);
        });

        try {
            do {
                $enabled = stream_socket_enable_crypto($this->sock, true);
                if ($enabled === true) {
                    return;
                }
                usleep(1000);
            } while ($enabled === 0 && time() < $timeout_at);
        } finally {
            restore_error_handler();
        }

        if ($enabled !== true) {
            throw new AMQPIOException('Can not enable crypto');
        }
    }

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

            $this->setErrorHandler();

            try {
                // With blocking sockets, fread() will block until data is available.
                // TrueAsync intercepts this blocking call and schedules other coroutines.
                $buffer = fread($this->sock, ($len - $read));

                if ($buffer === '') {
                    throw new AMQPNoDataException();
                }

                $this->throwOnError();
            } catch (\ErrorException $e) {
                throw new AMQPDataReadException($e->getMessage(), $e->getCode(), $e);
            } finally {
                $this->restoreErrorHandler();
            }

            if ($buffer === false) {
                throw new AMQPDataReadException('Error receiving data');
            }

            $this->last_read = microtime(true);
            $read            += mb_strlen($buffer, 'ASCII');
            $data            .= $buffer;
        }

        if (mb_strlen($data, 'ASCII') !== $len) {
            throw new AMQPDataReadException(
                sprintf(
                    'Error reading data. Received %s instead of expected %s bytes',
                    mb_strlen($data, 'ASCII'),
                    $len
                )
            );
        }

        $this->last_read = microtime(true);

        return $data;
    }

    public function write($data)
    {
        $this->checkBrokerHeartbeat();

        if (!is_resource($this->sock) || feof($this->sock)) {
            $this->close();
            $constants = SocketConstants::getInstance();
            throw new AMQPConnectionClosedException('Broken pipe or closed connection', $constants->SOCKET_EPIPE);
        }

        $this->setErrorHandler();

        try {
            fwrite($this->sock, $data);
            $this->throwOnError();
        } catch (\ErrorException $e) {
            $code      = $this->last_error['errno'];
            $constants = SocketConstants::getInstance();

            switch ($code) {
                case $constants->SOCKET_EPIPE:
                case $constants->SOCKET_ENETDOWN:
                case $constants->SOCKET_ENETUNREACH:
                case $constants->SOCKET_ENETRESET:
                case $constants->SOCKET_ECONNABORTED:
                case $constants->SOCKET_ECONNRESET:
                case $constants->SOCKET_ECONNREFUSED:
                case $constants->SOCKET_ETIMEDOUT:
                    $this->close();
                    throw new AMQPConnectionClosedException(socket_strerror($code), $code, $e);
                default:
                    throw new AMQPRuntimeException($e->getMessage(), $code, $e);
            }
        } finally {
            $this->restoreErrorHandler();
        }

        $this->last_write = microtime(true);
    }

    public function error_handler($errno, $errstr, $errfile, $errline, $errcontext = null)
    {
        $code      = $this->extractErrorCode($errstr);
        $constants = SocketConstants::getInstance();

        switch ($code) {
            case $constants->SOCKET_EAGAIN:
            case $constants->SOCKET_EWOULDBLOCK:
            case $constants->SOCKET_EINTR:
                return;
        }

        parent::error_handler($code > 0 ? $code : $errno, $errstr, $errfile, $errline, $errcontext);
    }

    public function close()
    {
        $this->disableHeartbeat();

        if (is_resource($this->sock)) {
            fclose($this->sock);
        }

        $this->sock       = null;
        $this->last_read  = 0;
        $this->last_write = 0;
    }

    protected function do_select(?int $sec, int $usec)
    {
        if ($this->sock === null || !is_resource($this->sock)) {
            $this->sock = null;
            throw new AMQPConnectionClosedException('Broken pipe or closed connection', 0);
        }

        $read   = [$this->sock];
        $write  = null;
        $except = null;

        if ($sec === null) {
            $usec = 0;
        }

        // With blocking sockets and TrueAsync, stream_select() works normally.
        // TrueAsync intercepts the blocking select call just like fread/fwrite.
        return stream_select($read, $write, $except, $sec, $usec);
    }

    protected function throwOnError(): void
    {
        if (is_resource($this->sock)) {
            $info = stream_get_meta_data($this->sock);

            if (!empty($info['timed_out'])) {
                throw new AMQPTimeoutException('Operation timed out');
            }
        }

        parent::throwOnError();
    }

    private function extractErrorCode(string $message): int
    {
        if (str_starts_with($message, 'stream_select():')) {
            $pattern = '/\s+\[(\d+)\]:\s+/';
        } else {
            $pattern = '/\s+errno=(\d+)\s+/';
        }

        $matches = [];
        $result  = preg_match($pattern, $message, $matches);

        if ($result > 0) {
            return (int) $matches[1];
        }

        return 0;
    }
}
