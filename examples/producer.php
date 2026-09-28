<?php

use Adsniper\KafkaRest\Rest\QueueRestClient;
use Adsniper\KafkaRest\Rest\QueueRestProducer;
use GuzzleHttp\Client;

$client = new QueueRestClient(
    new Client(['timeout'  => 30.0, 'verify' => false]),
    ['host' => 'localhost:18082']
);

$producer = new QueueRestProducer($client);
$producer->setConfig([
    'topic' => 'test',
    'group' => 'test',
]);

try {
    $producer->produceMessage(['text' => 'test producer ' . date('Y-m-d H:i:s')]);
} catch (\Exception $ex) {
    echo $ex;
}