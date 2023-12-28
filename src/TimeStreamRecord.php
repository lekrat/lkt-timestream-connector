<?php

namespace Lkt\Connectors;

class TimeStreamRecord
{

    protected const TIME_UNIT_MILLISECONDS = 'MILLISECONDS';
    protected const TIME_UNIT_SECONDS = 'SECONDS';
    protected const TIME_UNIT_MICROSECONDS = 'MICROSECONDS';
    protected const TIME_UNIT_NANOSECONDS = 'NANOSECONDS';

    protected array $dimensions = [];

    protected string $name = '';
    protected bool $useName = true;
    protected string $value = '';
    protected array $values = [];

    protected string $time = '';

    protected string $timeUnit = self::TIME_UNIT_SECONDS;

    public static function define(string $name, string $value, int $time): static
    {
        $r = new static();
        $r->name = $name;
        $r->value = $value;
        $r->time = (string)$time;
        return $r;
    }

    public static function defineMulti($name, array $record): static
    {
        $r = new static();

        $time = $record['_aws_time'] ?? time();
        $dimensions = is_array($record['_aws_dimensions']) ? $record['_aws_dimensions'] : [];

        unset($record['_aws_time']);
        unset($record['_aws_dimensions']);

        foreach ($dimensions as $dimension => $dimensionValue) $r->setDimension($dimension, $dimensionValue);

        if (isset($record['_aws_name'])) {
            $name = trim($record['_aws_name']);
        } else {
            $r->useName = false;
        }
        unset($record['_aws_name']);
        $r->name = $name;
        $r->values = $record;
        $r->time = (string)$time;
        return $r;
    }

    public function setDimension(string $name, string $value): static
    {
        $this->dimensions[$name] = $value;
        return $this;
    }

    public function setTimeUnitTiMilliseconds(): static
    {
        $this->timeUnit = static::TIME_UNIT_MILLISECONDS;
        return $this;
    }

    public function setTimeUnitTiSeconds(): static
    {
        $this->timeUnit = static::TIME_UNIT_SECONDS;
        return $this;
    }

    public function setTimeUnitTiMicroseconds(): static
    {
        $this->timeUnit = static::TIME_UNIT_MICROSECONDS;
        return $this;
    }

    public function setTimeUnitTiNanoseconds(): static
    {
        $this->timeUnit = static::TIME_UNIT_NANOSECONDS;
        return $this;
    }

    public function toWriteClient(): array
    {
        $dimensions = [];
        $key = [$this->name];
        foreach ($this->dimensions as $dimension => $value) {
            $dimensions[] = ['Name' => $dimension, 'Value' => $value];
            $key[] = $dimension;
            $key[] = $value;
        }

        $key = implode('-', $key);

        $r = [
            'Dimensions' => $dimensions,
            'Time' => $this->time,
            'TimeUnit' => $this->timeUnit,
            'Version' => time(),
        ];

        if ($this->values) {
            $r['MeasureName'] = $this->useName ? $this->name : $key;
            $r['MeasureValueType'] = 'MULTI';
            $r['MeasureValues'] = [];
            foreach ($this->values as $key => $value) {

                $type = 'DOUBLE';
                if (is_bool($value)) $type = 'BOOLEAN';
                if (is_numeric($value)) $type = 'DOUBLE';
                if (is_int($value)) $type = 'BIGINT';
                if (is_string($value)) $type = 'VARCHAR';

                $r['MeasureValues'][] = [
                    'Name' => $key,
                    'Type' => $type,
                    'Value' => (string)$value,
                ];
            }
        } else {
            $r['MeasureName'] = $this->name;
            $r['MeasureValue'] = $this->value;
        }

        return $r;
    }

    public static function fromDataRecordsToWriteClient(array $records, bool $isMulti): array
    {
        $r = [];
        foreach ($records as $i => $record) {
            if ($isMulti) {
                $aux = static::defineMulti($i, $record);
            } else {
                $aux = static::define(trim($record['name']), $record['value'], $record['_aws_time']);
                $dimensions = is_array($record['_aws_dimensions']) ? $record['_aws_dimensions'] : [];

                foreach ($dimensions as $dimension => $dimensionValue) $aux->setDimension($dimension, $dimensionValue);
            }
            $r[] = $aux->toWriteClient();
        }

        return $r;
    }
}