<?php

namespace Adsniper\KafkaRest;

interface ConsumerInterface
{
    public function consume(): int;

    public function setMessageHandler(
        QueueMessageHandlerInterface $queueMessageHandler
    ): ConsumerInterface;

    public function close(): void;

    /**
     * @param array<string,int|string|bool> $config
     */
    public function setConfig(array $config): void;

    public function isTimeOver(): bool;
}
