# Configure a connector
```php
use Lkt\Connectors\TimeStreamConnector;

TimeStreamConnector::define('aws')
    ->setUser('user')
    ->setPassword('password')
    ->setDatabase('database')
    ->setRegion('us-west-2');
```

## Read records

```php
use Lkt\Connectors\TimeStreamConnector;

TimeStreamConnector::get('aws')->query('your sql query');
```

## Write records

```php
use Lkt\Connectors\TimeStreamConnector;

TimeStreamConnector::get('aws')->write('table name', $records);
```

To add records simply add an array of arrays with a key-value format:

```php
$records = [

    [
        'v1' => 123,
        'v2' => 345,
        'v3' => 999,
    ]
];
```

This structure will create a record with multiple values for each variable in each record.

