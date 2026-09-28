<?php

namespace Adsniper\KafkaRest;

use RuntimeException;
use Throwable;

class QueueException extends RuntimeException
{
    public function __construct(
        string $message,
        int $code,
        ?Throwable $previous = null
    ) {
        parent::__construct($message, $code, $previous);
    }
}
