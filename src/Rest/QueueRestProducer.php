<?php

namespace Adsniper\KafkaRest\Rest;

use Adsniper\KafkaRest\ProducerInterface;

class QueueRestProducer extends AbstractQueueRestService implements ProducerInterface
{
    /**
     * @param array<mixed> $message
     */
    public function produceMessage(array $message, ?string $key = null): void
    {
        $this->client->exec(
            'POST',
            '/topics/' . $this->getTopic(),
            [
                'records' => [
                    [
                        'key' => $key,
                        'value' => $message
                    ]
                ]
            ],
            [
                'Content-Type' => 'application/vnd.kafka.json.v2+json',
                'Accept' => 'application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json',
            ]
        );
    }
}
