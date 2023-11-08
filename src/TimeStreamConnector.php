<?php

namespace Lkt\Connectors;

use Aws\Credentials\Credentials;
use Aws\Sdk;

class TimeStreamConnector
{
    protected ?Sdk $connection = null;
    protected string $user = '';
    protected string $password = '';
    protected string $region = '';
    protected string $database = '';

    protected const TIME_UNIT_MILLISECONDS = 'MILLISECONDS';
    protected const TIME_UNIT_SECONDS = 'SECONDS';
    protected const TIME_UNIT_MICROSECONDS = 'MICROSECONDS';
    protected const TIME_UNIT_NANOSECONDS = 'NANOSECONDS';

    /** @var TimeStreamConnector[] */
    protected static array $connectors = [];

    public static function define(string $name): static
    {
        $r = new static($name);
        static::$connectors[$name] = $r;
        return $r;
    }

    public static function get(string $name): static
    {
        if (!isset(static::$connectors[$name])) {
            throw new \Exception("Connector '{$name}' doesn't exists");
        }
        return static::$connectors[$name];
    }

    public function setUser(string $user): static
    {
        $this->user = $user;
        return $this;
    }

    public function setPassword(string $password): static
    {
        $this->password = $password;
        return $this;
    }

    public function setDatabase(string $database): static
    {
        $this->database = $database;
        return $this;
    }

    public function setRegion(string $region): static
    {
        $this->region = $region;
        return $this;
    }

    public function connect(): static
    {
        if ($this->connection !== null) {
            return $this;
        }

        // Perform the connection
        $credentials = new Credentials($this->user, $this->password);
        $this->connection = new Sdk(['region' => $this->region, 'credentials' => $credentials]);
        return $this;
    }

    public function disconnect(): static
    {
        $this->connection = null;
        return $this;
    }

    public function getQueryClient(): ?\Aws\TimestreamQuery\TimestreamQueryClient
    {
        return $this->connect()->connection?->createTimestreamQuery();
    }

    public function getWriteClient(): ?\Aws\TimestreamWrite\TimestreamWriteClient
    {
        return $this->connect()->connection?->createTimestreamWrite();
    }

    public function query(string $query): ?array
    {
        $client = $this->getQueryClient();
        $r = $client->query([
            'QueryString' => $query
        ]);

        return $r;
    }

    public function write(string $table, array $records): \Aws\Result
    {
        $records = [
            [
                'Dimensions' => [
                    [
                        'Name' => 'test',
                        'Value' => '1',
                    ]
                ],
                'MeasureName' => 't1',
                'MeasureValue' => '500',
                'Time' => (string)time(),
                'TimeUnit' => static::TIME_UNIT_SECONDS
            ]
        ];
        $payload = [
            'DatabaseName' => $this->database,
            'TableName' => $table,
            'Records' => $records,
        ];
        $client = $this->getWriteClient();
        return $client->writeRecords($payload);
    }
}