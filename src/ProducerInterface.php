<?php

namespace Adsniper\KafkaRest;

interface ProducerInterface
{
    /**
     * @param array<int|string,mixed> $message
     */
    public function produceMessage(array $message, ?string $key = null): void;

    /**
     * @param array<string,int|string|bool> $config
     */
    public function setConfig(array $config): void;
}
