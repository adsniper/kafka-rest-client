<?php

namespace Adsniper\KafkaRest\Rest;

use Adsniper\KafkaRest\QueueException;
use Adsniper\KafkaRest\QueueExceptionCodes;

abstract class AbstractQueueRestService
{
    /** @var QueueRestClient */
    protected $client;

    /** @var array<string,int|string|bool> */
    protected $config = [];

    public function __construct(QueueRestClient $client)
    {
        $this->client = $client;
    }

    /**
     * @param array<string,int|string|bool> $config
     * @return void
     */
    public function setConfig(array $config): void
    {
        $this->config = array_replace($this->config, $config);
    }

    protected function getTopic(): string
    {
        $topic = (string) ($this->config['topic'] ?? '');

        if (!$topic) {
            throw new QueueException(
                'Не установлен топик',
                QueueExceptionCodes::CONFIG_ERROR
            );
        }

        return $topic;
    }

    protected function getGroup(): string
    {
        return (string) ($this->config['group'] ?? null);
    }

    /**
     * @return array<string,string>
     */
    protected function getConsumerInstanceConfig(): array
    {
        return [
            'name' => $this->getDefaultСonsumerInstance(),
            'format' => 'json',
            'auto.offset.reset' => (string) ($this->config['auto.offset.reset'] ?? 'earliest'),
            'auto.commit.enable' => (string) ($this->config['enable.auto.commit'] ?? 'false'),
        ];
    }

    protected function getDefaultСonsumerInstance(): string
    {
        $consumerInstance = $this->config['consumerInstance'] ?? null;

        if (!$consumerInstance) {
            throw new QueueException(
                'Не установлена идентификатор консьюмера',
                QueueExceptionCodes::CONFIG_ERROR
            );
        }

        return (string) $consumerInstance;
    }
}
