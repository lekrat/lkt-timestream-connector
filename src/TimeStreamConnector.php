<?php

namespace Lkt\Connectors;

use Aws\Credentials\Credentials;
use Aws\Result;
use Aws\Sdk;

class TimeStreamConnector
{
    protected ?Sdk $connection = null;
    protected string $user = '';
    protected string $password = '';
    protected string $region = '';
    protected string $version = '';
    protected string $database = '';

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

    public function setVersion(string $version): static
    {
        $this->version = $version;
        return $this;
    }

    public function connect(): static
    {
        if ($this->connection !== null) {
            return $this;
        }

        // Perform the connection
        $credentials = new Credentials($this->user, $this->password);
        $sdkData = ['region' => $this->region, 'credentials' => $credentials];
        if ($this->version !== '') $sdkData['version'] = $this->version;
        $this->connection = new Sdk($sdkData);
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
        $result = $client->query(['QueryString' => $query]);

        $columnInfo = $result->getIterator()['ColumnInfo'];
        $rows = $result->getIterator()['Rows'];
        return array_map(function ($point) use ($columnInfo) {
            $r = [];
            foreach ($columnInfo as $i => $column) {
                $r[$column['Name']] = $point['Data'][$i]['ScalarValue'];
            }
            return $r;
        }, $rows);
    }

    public function write(string $table, array $records): \Aws\Result
    {
        $client = $this->getWriteClient();

        if (count($records) <= 100) {
            $payload = [
                'DatabaseName' => $this->database,
                'TableName' => $table,
                'Records' => TimeStreamRecord::fromDataRecordsToWriteClient($records),
            ];
            return $client->writeRecords($payload);
        }
        $toStore = array_chunk($records, 100);
        $r = null;
        foreach ($toStore as $data) {
            $r = $client->writeRecords([
                'DatabaseName' => $this->database,
                'TableName' => $table,
                'Records' => TimeStreamRecord::fromDataRecordsToWriteClient($data),
            ]);
        }
        return $r;
    }

    public function last(string $table, string $where = ''): ?array
    {
        $query = "SELECT * FROM \"{$this->database}\".\"{$table}\" :where ORDER BY time DESC LIMIT 1";
        if ($where !== '') {
            $query = str_replace(':where', " WHERE {$where}", $query);
        } else {
            $query = str_replace(':where', '', $query);
        }
        $r = $this->query($query);
        if (count($r) > 0) return $r[0];
        return null;
    }

    public function first(string $table, string $where = ''): ?array
    {
        $query = "SELECT * FROM \"{$this->database}\".\"{$table}\" :where ORDER BY time ASC LIMIT 1";
        if ($where !== '') {
            $query = str_replace(':where', " WHERE {$where}", $query);
        } else {
            $query = str_replace(':where', '', $query);
        }
        $r = $this->query($query);
        if (count($r) > 0) return $r[0];
        return null;
    }

    public function createDatabase(string $name): Result
    {
        $payload = [
            'DatabaseName' => $name
        ];
        $client = $this->getWriteClient();
        return $client->createDatabase($payload);
    }

    public function deleteDatabase(string $name): Result
    {
        $payload = [
            'DatabaseName' => $name
        ];
        $client = $this->getWriteClient();
        return $client->deleteDatabase($payload);
    }

    public function describeDatabase(string $name): Result
    {
        $payload = [
            'DatabaseName' => $name
        ];
        $client = $this->getWriteClient();
        return $client->describeDatabase($payload);
    }

    public function existsDatabase(string $name): bool
    {
        $payload = [
            'DatabaseName' => $name
        ];
        $client = $this->getWriteClient();

        try {
            $result = $client->describeDatabase($payload);
        } catch (\Exception $e) {
            return false;
        }
        return true;
    }

    public function createTable(string $table, int $memoryRetentionInHours = 8766, int $magneticRetentionInDays = 73000, bool $enableMagneticStoreWrites = true): Result
    {
        $payload = [
            'DatabaseName' => $this->database,
            'TableName' => $table,
            'MagneticStoreWriteProperties' => [
                'EnableMagneticStoreWrites' => $enableMagneticStoreWrites,
            ],
            'RetentionProperties' => [
                'MagneticStoreRetentionPeriodInDays' => $magneticRetentionInDays,
                'MemoryStoreRetentionPeriodInHours' => $memoryRetentionInHours,
            ]
        ];
        $client = $this->getWriteClient();
        return $client->createTable($payload);
    }

    public function describeTable(string $table): Result
    {
        $payload = [
            'DatabaseName' => $this->database,
            'TableName' => $table,
        ];
        $client = $this->getWriteClient();
        return $client->describeTable($payload);
    }

    public function existsTable(string $table): bool
    {
        $payload = [
            'DatabaseName' => $this->database,
            'TableName' => $table,
        ];
        $client = $this->getWriteClient();

        try {
            $result = $client->describeTable($payload);
        } catch (\Exception $e) {
            return false;
        }
        return true;
    }

    public function deleteTable(string $table): Result
    {
        $payload = [
            'DatabaseName' => $this->database,
            'TableName' => $table,
        ];
        $client = $this->getWriteClient();
        return $client->deleteTable($payload);
    }
}