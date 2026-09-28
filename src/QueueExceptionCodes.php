<?php

namespace Adsniper\KafkaRest;

final class QueueExceptionCodes
{
    /** 1-9 common errors */
    public const CONFIG_ERROR = 1;

    /** 10-19 Producer errors */
    public const MESSAGE_PRODUCE_ERROR = 10;

    /** 20-29 Consumer errors */
    public const MESSAGE_CONSUMING_TIMEOUT = 20;
    public const MESSAGE_ERROR_PROCESSING = 21;
}
