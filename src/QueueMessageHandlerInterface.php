<?php

namespace Adsniper\KafkaRest;

interface QueueMessageHandlerInterface
{
    /**
     * @param array<int|string,mixed> $message
     */
    public function handle(string $messageKey, array $message): bool;
}
