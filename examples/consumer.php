<?php

use Adsniper\KafkaRest\QueueMessageHandlerInterface;
use Adsniper\KafkaRest\Rest\QueueRestClient;
use Adsniper\KafkaRest\Rest\QueueRestConsumer;
use GuzzleHttp\Client;

class TextMessageHandler implements QueueMessageHandlerInterface
{
    /**
     * @param array<int|string,mixed> $message
     */
    public function handle(string $messageKey, array $message): bool
    {
        var_dump([
            '$messageKey' => $messageKey,
            '$message' => $message,
        ]);

        $success = false;

        if (isset($message['text'])) {
            // TODO
            $success = true;
        }

        return $success;
    }
}


$client = new QueueRestClient(
    new Client(['timeout'  => 30.0, 'verify' => false]),
    ['host' => 'localhost:18082']
);

$config = [
    'topic' => 'test',
    'group' => 'test',
    'consumerInstance' => 'test_instance_1',
    'auto.offset.reset' => 'earliest',
    'enable.auto.commit' => 'false',
    'timeout' => 180,
    ''
];

$consumer = new QueueRestConsumer($client);
$consumer->setConfig($config);
$consumer->setMessageHandler(
    new TextMessageHandler()
);

try {
    $consumer->close();

    while (!$consumer->isTimeOver()) {
        $consumer->consume();
        sleep(1);
    }
} catch (\Exception $ex) {
    echo $ex;
} finally {
    $consumer->close();
}