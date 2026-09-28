<?php

namespace Adsniper\KafkaRest\Rest;

use Exception;
use Psr\Log\NullLogger;
use GuzzleHttp\Psr7\Request;
use Psr\Log\LoggerInterface;
use GuzzleHttp\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use GuzzleHttp\Exception\RequestException;
use Psr\Http\Client\ClientExceptionInterface;
use Psr\Http\Client\NetworkExceptionInterface;
use Psr\Http\Client\RequestExceptionInterface;

class QueueRestClient
{
    /** @var array<string,int|string|bool> */
    private $config = [
        'dev' => false
    ];

    /** @var ClientInterface */
    private $httpClient;

    /** @var LoggerInterface */
    private $logger;

    /**
     * @param array<string,int|string|bool> $config
     */
    public function __construct(ClientInterface $client, array $config = [])
    {
        $this->httpClient = $client;
        $this->config = array_merge($this->config, $config);
        $this->logger = new NullLogger();
    }

    public function setLogger(LoggerInterface $logger): self
    {
        $this->logger = $logger;

        return $this;
    }

    private function getHost(): string
    {
        return (string) ($this->config['host'] ?? '');
    }

    /**
     * @param array<int|string,mixed> $data
     * @param array<int|string,string> $headers
     */
    public function exec(
        string $method,
        string $uri,
        array $data = [],
        array $headers = []
    ): string {
        if ($this->config['dev'] && empty($this->getHost())) {
            return '{}';
        }

        try {
            $request = new Request(
                $method,
                $this->getHost() . $uri,
                $headers,
                empty($data) ? null : (string) json_encode($data)
            );

            $response = $this->httpClient->send($request);

            if ($response->getStatusCode() > 204) {
                $this->handleError($request, $response);
            }

            $this->logger->debug(
                'CALL ' . (string) $request->getUri(),
                [
                    'request' => [
                        'method' => $request->getMethod(),
                        'uri' => (string) $request->getUri(),
                        'headers' => $request->getHeaders(),
                        'body' => (string) $request->getBody(),
                    ],
                    'response' => (string) $response->getBody()
                ]
            );

            return (string) $response->getBody();
        } catch (ClientExceptionInterface $ex) {
            $context = [];

            if ($ex instanceof NetworkExceptionInterface || $ex instanceof RequestExceptionInterface) {
                $context['request'] = [
                    'method' => $ex->getRequest()->getMethod(),
                    'uri' => (string) $ex->getRequest()->getUri(),
                    'headers' => $ex->getRequest()->getHeaders(),
                    'body' => (string) $ex->getRequest()->getBody(),
                ];
            }

            $this->logger->error((string) $ex, $context);
            throw $ex;
        } catch (Exception $ex) {
            $this->logger->error((string) $ex);
            throw $ex;
        }
    }

    private function handleError(RequestInterface $request, ResponseInterface $response): void
    {
        $message = (string) $response->getBody();
        $decodedMessage = (array) json_decode($message, true);

        $messageCode = (isset($decodedMessage['error_code']) && is_scalar($decodedMessage['error_code'])) ? (string) $decodedMessage['error_code'] : null;
        $statusCode = $response->getStatusCode();

        switch ($statusCode) {
            case 401:
                $messageCode = $messageCode ?? '401';
                $message = $message ?: 'Kafka Authentication Error';

                break;
            case 403:
                $messageCode =  $messageCode ?? '403';
                $message = $message ?: 'Kafka Authorization Error';

                break;
            case 404:
                $messageCode = $messageCode ?? '404';
                $message = $message ?: 'Not Found';

                break;
            case 406:
                $messageCode = $messageCode ?? '406';
                $message = $message ?: 'Not Acceptable';

                break;
            case 408:
                $messageCode = $messageCode ?? '408';
                $message = $message ?: 'Request Timeout';

                break;
            case 409:
                $messageCode = $messageCode ?? '409';
                $message = $message ?: 'Conflict';

                break;
            case 422:
                $messageCode = $messageCode ?? '422';
                $message = $message ?: 'Unprocessable Entity';

                break;
            case 500:
                $messageCode = $messageCode ?? '500';
                $message = $message ?: 'Internal Server Error';

                break;
            default:
                $messageCode = 'unknown';
                $message = $message ?: 'Unknown Error';

                break;
        }

        throw new RequestException(
            sprintf(
                "Error %s: %s",
                (string) $messageCode,
                $message
            ),
            $request,
            $response
        );
    }
}
