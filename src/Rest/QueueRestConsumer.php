<?php

namespace Adsniper\KafkaRest\Rest;

use Adsniper\KafkaRest\ConsumerInterface;
use Adsniper\KafkaRest\QueueException;
use Adsniper\KafkaRest\QueueExceptionCodes;
use Adsniper\KafkaRest\QueueMessageHandlerInterface;
use GuzzleHttp\Exception\ClientException;

class QueueRestConsumer extends AbstractQueueRestService implements ConsumerInterface
{
    /** @var int */
    private const TIMEOUT_PER_MESSAGE_MS = 1000;

    /** @var int */
    private const MAX_MESSAGES = 100;

    /** @var int */
    private const MAX_CONSUME_BYTES = 150000;

    /** @var ?QueueMessageHandlerInterface */
    private $handler;

    /** @var float */
    private $startTime = 0;

    /** @var string */
    private $consumerInstance;

    /** @var bool */
    private $init = false;

    /** @var int */
    private $timeout = 60;

    public function consume(): int
    {
        if (!isset($this->handler)) {
            throw new QueueException(
                'Message handler must be set',
                QueueExceptionCodes::CONFIG_ERROR
            );
        }

        if (!$this->init) {
            $this->startTime = microtime(true);
            $this->subscribeToTopic();

            $this->init = true;
        }

        $request = sprintf(
            "/consumers/%s/instances/%s/records?max_bytes=%d&timeout=%d",
            $this->getGroup(),
            $this->consumerInstance,
            self::MAX_CONSUME_BYTES,
            self::TIMEOUT_PER_MESSAGE_MS
        );

        $response = $this->client->exec(
            'GET',
            $request,
            [],
            ['Accept' => 'application/vnd.kafka.json.v2+json']
        );
        /** @var list<array{key:string,value:array<int|string,mixed>,partition?:string,offset?:string}> */
        $messages = json_decode($response, true);

        return $this->processMessages($messages);
    }

    /**
     * @param array<string,int|string|bool> $config
     */
    public function setConfig(array $config): void
    {
        $this->init = false;
        parent::setConfig($config);
        $this->timeout = (int) ($this->config['timeout'] ?? $this->timeout);
    }

    /**
     * @param list<array{key:string,value:array<int|string,mixed>,partition?:string,offset?:string}> $messages
     */
    private function processMessages(array $messages): int
    {
        $processedMessages = 0;

        foreach ($messages as $message) {
            if ($this->isTimeOver()) {
                break;
            }

            $status = $this->handler?->handle((string) $message['key'], (array) $message['value']);

            if ($status && isset($message['partition']) && isset($message['offset'])) {
                $this->commit((string) $message['partition'], (string) $message['offset']);
            }

            $processedMessages++;

            if ($processedMessages === self::MAX_MESSAGES) {
                break;
            }
        }

        return $processedMessages;
    }

    private function createConsumerInstance(): void
    {
        if (!empty($this->consumerInstance)) {
            return;
        }

        $data = $this->client->exec(
            'POST',
            "/consumers/{$this->getGroup()}/",
            $this->getConsumerInstanceConfig(),
            ['Content-Type' => 'application/vnd.kafka.v2+json']
        );

        $res = (array) json_decode($data, true);
        $this->consumerInstance = is_string($res['instance_id']) ? (string) $res['instance_id'] : '';
    }

    private function subscribeToTopic(): void
    {
        $this->createConsumerInstance();

        $this->client->exec(
            "POST",
            "/consumers/{$this->getGroup()}/instances/{$this->consumerInstance}/subscription",
            [
                'topics' => [$this->getTopic()],
            ],
            ['Content-Type' => 'application/vnd.kafka.v2+json']
        );
    }

    private function commit(string $partition, string $offset): void
    {
        $this->client->exec(
            'POST',
            "/consumers/{$this->getGroup()}/instances/{$this->consumerInstance}/offsets",
            [
                'offsets' => [
                    [
                        'topic' => $this->getTopic(),
                        'partition' => $partition,
                        'offset' => $offset,
                    ]
                ]
            ],
            ['Content-Type' => 'application/vnd.kafka.v2+json']
        );
    }

    public function close(): void
    {
        try {
            $this->client->exec(
                'DELETE',
                "/consumers/{$this->getGroup()}/instances/{$this->getDefaultСonsumerInstance()}",
                ['Content-Type' => 'application/vnd.kafka.v2+json']
            );
        } catch (ClientException $ex) {
            if ($ex->getCode() === 404) {
                return;
            }

            throw $ex;
        }

        $this->init = false;
    }

    public function isTimeOver(): bool
    {
        if ((float) $this->startTime === (float) 0) {
            return false;
        }

        return (microtime(true) - $this->startTime) >= $this->timeout;
    }

    public function setMessageHandler(
        QueueMessageHandlerInterface $queueMessageHandler
    ): ConsumerInterface {
        $this->handler = $queueMessageHandler;

        return $this;
    }
}
