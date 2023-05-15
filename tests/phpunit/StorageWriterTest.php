<?php

declare(strict_types=1);

namespace Keboola\StorageWriter\Tests;

use Exception;
use Keboola\Component\UserException;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageWriter\Component;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Psr\Log\Test\TestLogger;
use Symfony\Component\Filesystem\Filesystem;

class StorageWriterTest extends TestCase
{
    private Client $client;

    public function setUp(): void
    {
        parent::setUp();
        if (empty(getenv('KBC_TEST_TOKEN')) || empty(getenv('KBC_TEST_URL'))
            || empty(getenv('KBC_TEST_BUCKET'))
        ) {
            throw new Exception('KBC_TEST_TOKEN, KBC_TEST_URL or KBC_TEST_BUCKET is empty');
        }
        $this->client = new Client([
            'token' => getenv('KBC_TEST_TOKEN'),
            'url' => getenv('KBC_TEST_URL'),
        ]);
        $tables = $this->client->listTables(getenv('KBC_TEST_BUCKET'));
        foreach ($tables as $table) {
            $this->client->dropTable($table['id']);
        }
    }

    public function testCreateTable(): void
    {
        $temp = new Temp('wr-storage');
        $baseDir = $temp->getTmpFolder();
        $fs = new Filesystem();
        $fs->mkdir($baseDir . '/in/tables/');
        $tableName = $baseDir . '/in/tables/some-table-1';
        $fs->dumpFile($tableName, "\"id\",\"name\"\n\"1\",\"Bar\"\n\"2\",\"Kochba\"\n\"3\",\"Foo\"\n");
        $manifest = [
            'primary_key' => ['id'],
        ];
        $fs->dumpFile($tableName . '.manifest', (string) json_encode($manifest));
        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
            ],
            'storage' => [
                'input' => [
                    'tables' => [
                        [
                            'source' => 'in.c-main.some-source',
                            'destination' => 'some-table-1',
                        ],
                    ],
                ],
            ],
        ];
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $logger = new TestLogger();
        $app = new Component($logger);
        $app->execute();
        self::assertTrue($this->client->tableExists(getenv('KBC_TEST_BUCKET') . '.some-table-1'));
        $table = $this->client->getTable(getenv('KBC_TEST_BUCKET') . '.some-table-1');
        self::assertEquals(['id'], $table['primaryKey']);
        self::assertTrue($logger->hasInfoThatContains('Authorized for project'));
    }

    public function testAlreadyExists(): void
    {
        $temp = new Temp('wr-storage');
        $fs = new Filesystem();
        $fs->dumpFile($temp->getTmpFolder() . '/tmp.csv', "\"id\",\"name\"\n\"1\",\"a\"\n\"2\",\"b\"\n\"3\",\"c\"\n");
        $csv = new CsvFile($temp->getTmpFolder() . '/tmp.csv');
        $this->client->createTable(getenv('KBC_TEST_BUCKET'), 'some-table-2', $csv, ['primaryKey' => 'id']);

        $baseDir = $temp->getTmpFolder();
        $fs->mkdir($baseDir . '/in/tables/');
        $tableName = $baseDir . '/in/tables/some-table-2';
        $fs->dumpFile($tableName, "\"id\",\"name\"\n\"1\",\"Bar\"\n\"2\",\"Kochba\"\n\"3\",\"Foo\"\n");
        $manifest = [
            'primary_key' => ['id'],
        ];
        $fs->dumpFile($tableName . '.manifest', (string) json_encode($manifest));
        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
            ],
            'storage' => [
                'input' => [
                    'tables' => [
                        [
                            'source' => 'in.c-main.some-source',
                            'destination' => 'some-table-2',
                        ],
                    ],
                ],
            ],
        ];
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $app->execute();
        self::assertTrue($this->client->tableExists(getenv('KBC_TEST_BUCKET') . '.some-table-2'));
    }

    public function testWithBucketLegacy(): void
    {
        $temp = new Temp('wr-storage');
        $baseDir = $temp->getTmpFolder();
        $fs = new Filesystem();
        $fs->mkdir($baseDir . '/in/tables/');
        $tableName = $baseDir . '/in/tables/some-table-3';
        $fs->dumpFile($tableName, "\"id\",\"name\"\n\"1\",\"Bar\"\n\"2\",\"Kochba\"\n\"3\",\"Foo\"\n");
        $manifest = [
            'primary_key' => ['id'],
        ];
        $fs->dumpFile($tableName . '.manifest', (string) json_encode($manifest));
        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
                'bucket' => getenv('KBC_TEST_BUCKET'),
            ],
            'storage' => [
                'input' => [
                    'tables' => [
                        [
                            'source' => 'in.c-main.some-source',
                            'destination' => 'some-table-3',
                        ],
                    ],
                ],
            ],
        ];
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $app->execute();
        self::assertTrue($this->client->tableExists(getenv('KBC_TEST_BUCKET') . '.some-table-3'));
    }

    public function testInvalidToken(): void
    {
        $temp = new Temp('wr-storage');
        $baseDir = $temp->getTmpFolder();
        $fs = new Filesystem();
        $fs->mkdir($baseDir . '/in/tables/');
        $tableName = $baseDir . '/in/tables/some-table-1';
        $fs->dumpFile($tableName, "\"id\",\"name\"\n\"1\",\"Bar\"\n\"2\",\"Kochba\"\n\"3\",\"Foo\"\n");
        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => 'invalid',
                'url' => getenv('KBC_TEST_URL'),
            ],
            'storage' => [
                'input' => [
                    'tables' => [
                        [
                            'source' => 'in.c-main.some-source',
                            'destination' => 'some-table-4',
                        ],
                    ],
                ],
            ],
        ];
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        self::expectException(UserException::class);
        self::expectExceptionMessage('Invalid access token');
        $app->execute();
    }

    public function testInvalidMode(): void
    {
        $temp = new Temp('wr-storage');
        $baseDir = $temp->getTmpFolder();
        $fs = new Filesystem();
        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => 'invalid',
                'url' => getenv('KBC_TEST_URL'),
                'mode' => 'funky',
            ],
            'storage' => [],
        ];
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        self::expectException(UserException::class);
        self::expectExceptionMessage(
            'Invalid configuration for path "root.parameters.mode": Mode must be one of "recreate, replace, update"'
        );
        new Component(new NullLogger());
    }

    public function testIncremental(): void
    {
        $temp = new Temp('wr-storage');
        $baseDir = $temp->getTmpFolder();
        $csv = new CsvFile($baseDir . DIRECTORY_SEPARATOR . uniqid('csv'));
        $csv->writeRow(['id', 'name']);
        $csv->writeRow(['1', 'foo']);
        $csv->writeRow(['4', 'foobar']);
        $this->client->createTableAsync((string) getenv('KBC_TEST_BUCKET'), 'some-table-5', $csv);

        $fs = new Filesystem();
        $fs->mkdir($baseDir . '/in/tables/');
        $tableName = $baseDir . '/in/tables/some-table-5';
        $fs->dumpFile($tableName, "\"id\",\"name\"\n\"1\",\"Bar\"\n\"2\",\"Kochba\"\n\"3\",\"Foo\"\n");
        $manifest = [
            'primary_key' => [],
        ];
        $fs->dumpFile($tableName . '.manifest', (string) json_encode($manifest));
        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
                'incremental' => true,
            ],
            'storage' => [
                'input' => [
                    'tables' => [
                        [
                            'source' => 'in.c-main.some-source',
                            'destination' => 'some-table-5',
                        ],
                    ],
                ],
            ],
        ];
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $app->execute();
        self::assertTrue($this->client->tableExists(getenv('KBC_TEST_BUCKET') . '.some-table-5'));
        $data = $this->client->getTableDataPreview(getenv('KBC_TEST_BUCKET') . '.some-table-5');
        $data = explode("\n", $data);
        sort($data);
        self::assertEquals(
            [
                '',
                '"1","Bar"',
                '"1","foo"',
                '"2","Kochba"',
                '"3","Foo"',
                '"4","foobar"',
                '"id","name"',
            ],
            $data
        );
    }

    public function testIncrementalModeUpdate(): void
    {
        $temp = new Temp('wr-storage');
        $baseDir = $temp->getTmpFolder();
        $csv = new CsvFile($baseDir . DIRECTORY_SEPARATOR . uniqid('csv'));
        $csv->writeRow(['id', 'name']);
        $csv->writeRow(['1', 'foo']);
        $csv->writeRow(['4', 'foobar']);
        $this->client->createTableAsync((string) getenv('KBC_TEST_BUCKET'), 'some-table-5', $csv);

        $fs = new Filesystem();
        $fs->mkdir($baseDir . '/in/tables/');
        $tableName = $baseDir . '/in/tables/some-table-5';
        $fs->dumpFile($tableName, "\"id\",\"name\"\n\"1\",\"Bar\"\n\"2\",\"Kochba\"\n\"3\",\"Foo\"\n");
        $manifest = [
            'primary_key' => [],
        ];
        $fs->dumpFile($tableName . '.manifest', (string) json_encode($manifest));
        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
                'mode' => 'update',
            ],
            'storage' => [
                'input' => [
                    'tables' => [
                        [
                            'source' => 'in.c-main.some-source',
                            'destination' => 'some-table-5',
                        ],
                    ],
                ],
            ],
        ];
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $app->execute();
        self::assertTrue($this->client->tableExists(getenv('KBC_TEST_BUCKET') . '.some-table-5'));
        $data = $this->client->getTableDataPreview(getenv('KBC_TEST_BUCKET') . '.some-table-5');
        $data = explode("\n", $data);
        sort($data);
        self::assertEquals(
            [
                '',
                '"1","Bar"',
                '"1","foo"',
                '"2","Kochba"',
                '"3","Foo"',
                '"4","foobar"',
                '"id","name"',
            ],
            $data
        );
    }

    public function testEmptyManifest(): void
    {
        $temp = new Temp('wr-storage');
        $baseDir = $temp->getTmpFolder();
        $fs = new Filesystem();
        $fs->mkdir($baseDir . '/in/tables/');
        $tableName = $baseDir . '/in/tables/some-table-6';
        $fs->dumpFile($tableName, "\"id\",\"name\"\n\"1\",\"Bar\"\n\"2\",\"Kochba\"\n\"3\",\"Foo\"\n");
        $manifest = [];
        $fs->dumpFile($tableName . '.manifest', (string) json_encode($manifest));
        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
            ],
            'storage' => [
                'input' => [
                    'tables' => [
                        [
                            'source' => 'in.c-main.some-source',
                            'destination' => 'some-table-6',
                        ],
                    ],
                ],
            ],
        ];
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $app->execute();
        self::assertTrue($this->client->tableExists(getenv('KBC_TEST_BUCKET') . '.some-table-6'));
        $table = $this->client->getTable(getenv('KBC_TEST_BUCKET') . '.some-table-6');
        self::assertEquals([], $table['primaryKey']);
    }

    public function testAlreadyExistsWrongPk(): void
    {
        $temp = new Temp('wr-storage');
        $fs = new Filesystem();
        $fs->dumpFile($temp->getTmpFolder() . '/tmp.csv', "\"id\",\"name\"\n\"1\",\"a\"\n\"2\",\"b\"\n\"3\",\"c\"\n");
        $csv = new CsvFile($temp->getTmpFolder() . '/tmp.csv');
        $this->client->createTable(getenv('KBC_TEST_BUCKET'), 'some-table-7', $csv, ['primaryKey' => 'name,id']);

        $baseDir = $temp->getTmpFolder();
        $fs->mkdir($baseDir . '/in/tables/');
        $tableName = $baseDir . '/in/tables/some-table-7';
        $fs->dumpFile($tableName, "\"id\",\"name\"\n\"1\",\"Bar\"\n\"4\",\"b\"\n\"5\",\"c\"\n");
        $manifest = [
            'primary_key' => [],
        ];
        $fs->dumpFile($tableName . '.manifest', (string) json_encode($manifest));
        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
                'incremental' => true,
            ],
            'storage' => [
                'input' => [
                    'tables' => [
                        [
                            'source' => 'in.c-main.some-source',
                            'destination' => 'some-table-7',
                        ],
                    ],
                ],
            ],
        ];
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        self::expectException(UserException::class);
        self::expectExceptionMessage(
            'Primary key in the destination table "some-table-7" - ["name","id"] ' .
            'does not match the primary key in the source table - [].'
        );
        $app->execute();
    }

    public function testAlreadyExistsWrongColumns(): void
    {
        $temp = new Temp('wr-storage');
        $fs = new Filesystem();
        $fs->dumpFile($temp->getTmpFolder() . '/tmp.csv', "\"id\",\"boo\"\n\"1\",\"a\"\n\"2\",\"b\"\n");
        $csv = new CsvFile($temp->getTmpFolder() . '/tmp.csv');
        $this->client->createTable(getenv('KBC_TEST_BUCKET'), 'some-table-8', $csv, ['primaryKey' => 'id']);

        $baseDir = $temp->getTmpFolder();
        $fs->mkdir($baseDir . '/in/tables/');
        $tableName = $baseDir . '/in/tables/some-table-8';
        $fs->dumpFile($tableName, "\"id\",\"name\"\n\"1\",\"Bar\"\n\"4\",\"b\"\n\"5\",\"c\"\n");
        $manifest = [
            'primary_key' => ['id'],
        ];
        $fs->dumpFile($tableName . '.manifest', (string) json_encode($manifest));
        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
                'incremental' => false,
            ],
            'storage' => [
                'input' => [
                    'tables' => [
                        [
                            'source' => 'in.c-main.some-source',
                            'destination' => 'some-table-8',
                        ],
                    ],
                ],
            ],
        ];
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        self::expectException(UserException::class);
        self::expectExceptionMessage(
            'Some columns are missing in the csv file. Missing columns: boo. Expected columns: id,boo.'
        );
        $app->execute();
    }

    public function testAlreadyExistsWrongColumnsModeRecreate(): void
    {
        $temp = new Temp('wr-storage');
        $fs = new Filesystem();
        $fs->dumpFile($temp->getTmpFolder() . '/tmp.csv', "\"id\",\"boo\"\n\"1\",\"a\"\n\"2\",\"b\"\n");
        $csv = new CsvFile($temp->getTmpFolder() . '/tmp.csv');
        $this->client->createTable(getenv('KBC_TEST_BUCKET'), 'some-table-9', $csv, ['primaryKey' => 'id']);
        $tableInfo = $this->client->getTable(getenv('KBC_TEST_BUCKET') . '.some-table-9');
        self::assertEquals(['id', 'boo'], $tableInfo['columns']);

        $baseDir = $temp->getTmpFolder();
        $fs->mkdir($baseDir . '/in/tables/');
        $tableName = $baseDir . '/in/tables/some-table-9';
        $fs->dumpFile($tableName, "\"id\",\"name\"\n\"1\",\"Bar\"\n\"4\",\"b\"\n\"5\",\"c\"\n");
        $manifest = [
            'primary_key' => ['name'],
        ];
        $fs->dumpFile($tableName . '.manifest', (string) json_encode($manifest));
        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
                'mode' => 'recreate',
            ],
            'storage' => [
                'input' => [
                    'tables' => [
                        [
                            'source' => 'in.c-main.some-source',
                            'destination' => 'some-table-9',
                        ],
                    ],
                ],
            ],
        ];
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $app->execute();
        self::assertTrue($this->client->tableExists(getenv('KBC_TEST_BUCKET') . '.some-table-9'));
        $tableInfo = $this->client->getTable(getenv('KBC_TEST_BUCKET') . '.some-table-9');
        self::assertEquals(['id', 'name'], $tableInfo['columns']);
    }

    public function testNotExistsModeRecreate(): void
    {
        $temp = new Temp('wr-storage');
        $fs = new Filesystem();
        self::assertFalse($this->client->tableExists(getenv('KBC_TEST_BUCKET') . '.some-table-10'));

        $baseDir = $temp->getTmpFolder();
        $fs->mkdir($baseDir . '/in/tables/');
        $tableName = $baseDir . '/in/tables/some-table-10';
        $fs->dumpFile($tableName, "\"id\",\"name\"\n\"1\",\"Bar\"\n\"4\",\"b\"\n\"5\",\"c\"\n");
        $manifest = [
            'primary_key' => ['name'],
        ];
        $fs->dumpFile($tableName . '.manifest', (string) json_encode($manifest));
        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
                'mode' => 'recreate',
            ],
            'storage' => [
                'input' => [
                    'tables' => [
                        [
                            'source' => 'in.c-main.some-source',
                            'destination' => 'some-table-10',
                        ],
                    ],
                ],
            ],
        ];
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $app->execute();
        self::assertTrue($this->client->tableExists(getenv('KBC_TEST_BUCKET') . '.some-table-10'));
        $tableInfo = $this->client->getTable(getenv('KBC_TEST_BUCKET') . '.some-table-10');
        self::assertEquals(['id', 'name'], $tableInfo['columns']);
    }

    public function testAction(): void
    {
        $temp = new Temp('wr-storage');
        $baseDir = $temp->getTmpFolder();
        $fs = new Filesystem();
        $configFile = [
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
            ],
            'action' => 'info',
        ];
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $result = '';
        ob_start(function ($content) use (&$result): void {
            $result .= $content;
        });
        $app->execute();
        ob_end_clean();
        $data = (array) json_decode($result, true);
        $tokenInfo = $this->client->verifyToken();
        self::assertArrayHasKey('bucket', $data);
        self::assertArrayHasKey('projectId', $data);
        self::assertArrayHasKey('projectName', $data);
        self::assertEquals(getenv('KBC_TEST_BUCKET'), $data['bucket']);
        self::assertEquals($tokenInfo['owner']['id'], $data['projectId']);
        self::assertEquals($tokenInfo['owner']['name'], $data['projectName']);
    }

    public function testActionInvalidToken(): void
    {
        $temp = new Temp('wr-storage');
        $baseDir = $temp->getTmpFolder();
        $fs = new Filesystem();
        $configFile = [
            'parameters' => [
                '#token' => 'abcd',
                'url' => getenv('KBC_TEST_URL'),
            ],
            'action' => 'info',
        ];
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        self::expectException(UserException::class);
        self::expectExceptionMessage('Invalid access token');
        $app->execute();
    }

    public function testActionMissingToken(): void
    {
        $temp = new Temp('wr-storage');
        $baseDir = $temp->getTmpFolder();
        $fs = new Filesystem();
        $configFile = [
            'parameters' => [
                'url' => getenv('KBC_TEST_URL'),
            ],
            'action' => 'info',
        ];
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        self::expectException(UserException::class);
        self::expectExceptionMessage('Missing "parameters.#token" in configuration.');
        $app->execute();
    }
}
