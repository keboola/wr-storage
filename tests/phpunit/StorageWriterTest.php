<?php

declare(strict_types=1);

namespace Keboola\StorageWriter\Tests;

use Keboola\Component\UserException;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageWriter\Component;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Symfony\Component\Filesystem\Filesystem;

class StorageWriterTest extends TestCase
{
    /**
     * @var Client
     */
    private $client;

    public function setUp(): void
    {
        parent::setUp();
        if (empty(getenv('KBC_TEST_TOKEN')) || empty(getenv('KBC_TEST_URL'))
            || empty(getenv('KBC_TEST_BUCKET'))
        ) {
            throw new \Exception('KBC_TEST_TOKEN, KBC_TEST_URL or KBC_TEST_BUCKET is empty');
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

    public function testBasic(): void
    {
        $temp = new Temp('wr-storage');
        $temp->initRunFolder();
        $baseDir = $temp->getTmpFolder();
        $fs = new Filesystem();
        $fs->mkdir($baseDir . '/in/tables/');
        $tableName = $baseDir . '/in/tables/some-table-1';
        $fs->dumpFile($tableName, "\"id\",\"name\"\n\"1\",\"Bar\"\n\"2\",\"Kochba\"\n\"3\",\"Foo\"\n");
        $manifest = [
            'primary_key' => ['id'],
        ];
        $fs->dumpFile($tableName . '.manifest', \GuzzleHttp\json_encode($manifest));
        $configFile = [
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
        $fs->dumpFile($baseDir . '/config.json', \GuzzleHttp\json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $app->run();
        self::assertTrue($this->client->tableExists(getenv('KBC_TEST_BUCKET') . '.some-table-1'));
        $table = $this->client->getTable(getenv('KBC_TEST_BUCKET') . '.some-table-1');
        self::assertEquals(['id'], $table['primaryKey']);
    }

    public function testAlreadyExists(): void
    {
        $temp = new Temp('wr-storage');
        $temp->initRunFolder();
        $fs = new Filesystem();
        $fs->dumpFile($temp->getTmpFolder() . '/tmp.csv', "\"id\",\"name\"\n\"1\",\"a\"\n\"2\",\"b\"\n\"3\",\"c\"\n");
        $csv = new CsvFile($temp->getTmpFolder() . '/tmp.csv');
        $this->client->createTable(getenv('KBC_TEST_BUCKET'), 'some-table-2', $csv);

        $baseDir = $temp->getTmpFolder();
        $fs->mkdir($baseDir . '/in/tables/');
        $tableName = $baseDir . '/in/tables/some-table-2';
        $fs->dumpFile($tableName, "\"id\",\"name\"\n\"1\",\"Bar\"\n\"2\",\"Kochba\"\n\"3\",\"Foo\"\n");
        $manifest = [
            'primary_key' => ['id'],
        ];
        $fs->dumpFile($tableName . '.manifest', \GuzzleHttp\json_encode($manifest));
        $configFile = [
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
        $fs->dumpFile($baseDir . '/config.json', \GuzzleHttp\json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $app->run();
        self::assertTrue($this->client->tableExists(getenv('KBC_TEST_BUCKET') . '.some-table-2'));
    }

    public function testWithBucket(): void
    {
        $temp = new Temp('wr-storage');
        $temp->initRunFolder();
        $baseDir = $temp->getTmpFolder();
        $fs = new Filesystem();
        $fs->mkdir($baseDir . '/in/tables/');
        $tableName = $baseDir . '/in/tables/some-table-3';
        $fs->dumpFile($tableName, "\"id\",\"name\"\n\"1\",\"Bar\"\n\"2\",\"Kochba\"\n\"3\",\"Foo\"\n");
        $manifest = [
            'primary_key' => ['id'],
        ];
        $fs->dumpFile($tableName . '.manifest', \GuzzleHttp\json_encode($manifest));
        $configFile = [
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
        $fs->dumpFile($baseDir . '/config.json', \GuzzleHttp\json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $app->run();
        self::assertTrue($this->client->tableExists(getenv('KBC_TEST_BUCKET') . '.some-table-3'));
    }

    public function testInvalidToken(): void
    {
        $temp = new Temp('wr-storage');
        $temp->initRunFolder();
        $baseDir = $temp->getTmpFolder();
        $fs = new Filesystem();
        $fs->mkdir($baseDir . '/in/tables/');
        $tableName = $baseDir . '/in/tables/some-table-1';
        $fs->dumpFile($tableName, "\"id\",\"name\"\n\"1\",\"Bar\"\n\"2\",\"Kochba\"\n\"3\",\"Foo\"\n");
        $configFile = [
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
        $fs->dumpFile($baseDir . '/config.json', \GuzzleHttp\json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        self::expectException(UserException::class);
        self::expectExceptionMessage('Invalid access token');
        $app->run();
    }

    public function testIncremental(): void
    {
        $temp = new Temp('wr-storage');
        $temp->initRunFolder();
        $baseDir = $temp->getTmpFolder();
        $csv = new CsvFile($baseDir . DIRECTORY_SEPARATOR . uniqid('csv'));
        $csv->writeRow(['id', 'name']);
        $csv->writeRow(['1', 'foo']);
        $csv->writeRow(['4', 'foobar']);
        $this->client->createTableAsync(getenv('KBC_TEST_BUCKET'), 'some-table-5', $csv);

        $fs = new Filesystem();
        $fs->mkdir($baseDir . '/in/tables/');
        $tableName = $baseDir . '/in/tables/some-table-5';
        $fs->dumpFile($tableName, "\"id\",\"name\"\n\"1\",\"Bar\"\n\"2\",\"Kochba\"\n\"3\",\"Foo\"\n");
        $manifest = [
            'primary_key' => ['id'],
        ];
        $fs->dumpFile($tableName . '.manifest', \GuzzleHttp\json_encode($manifest));
        $configFile = [
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
        $fs->dumpFile($baseDir . '/config.json', \GuzzleHttp\json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $app->run();
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
        $temp->initRunFolder();
        $baseDir = $temp->getTmpFolder();
        $fs = new Filesystem();
        $fs->mkdir($baseDir . '/in/tables/');
        $tableName = $baseDir . '/in/tables/some-table-6';
        $fs->dumpFile($tableName, "\"id\",\"name\"\n\"1\",\"Bar\"\n\"2\",\"Kochba\"\n\"3\",\"Foo\"\n");
        $manifest = [];
        $fs->dumpFile($tableName . '.manifest', \GuzzleHttp\json_encode($manifest));
        $configFile = [
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
        $fs->dumpFile($baseDir . '/config.json', \GuzzleHttp\json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $app->run();
        self::assertTrue($this->client->tableExists(getenv('KBC_TEST_BUCKET') . '.some-table-6'));
        $table = $this->client->getTable(getenv('KBC_TEST_BUCKET') . '.some-table-6');
        self::assertEquals([], $table['primaryKey']);
    }

    public function testAlreadyExistsWrongPk(): void
    {
        $temp = new Temp('wr-storage');
        $temp->initRunFolder();
        $fs = new Filesystem();
        $fs->dumpFile($temp->getTmpFolder() . '/tmp.csv', "\"id\",\"name\"\n\"1\",\"a\"\n\"2\",\"b\"\n\"3\",\"c\"\n");
        $csv = new CsvFile($temp->getTmpFolder() . '/tmp.csv');
        $this->client->createTable(getenv('KBC_TEST_BUCKET'), 'some-table-7', $csv, ['primaryKey' => 'name']);

        $baseDir = $temp->getTmpFolder();
        $fs->mkdir($baseDir . '/in/tables/');
        $tableName = $baseDir . '/in/tables/some-table-7';
        $fs->dumpFile($tableName, "\"id\",\"name\"\n\"1\",\"Bar\"\n\"4\",\"b\"\n\"5\",\"c\"\n");
        $manifest = [
            'primary_key' => ['id'],
        ];
        $fs->dumpFile($tableName . '.manifest', \GuzzleHttp\json_encode($manifest));
        $configFile = [
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
        $fs->dumpFile($baseDir . '/config.json', \GuzzleHttp\json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $app->run();
        self::assertTrue($this->client->tableExists(getenv('KBC_TEST_BUCKET') . '.some-table-7'));
        $table = $this->client->getTable(getenv('KBC_TEST_BUCKET') . '.some-table-7');
        self::assertEquals(['name'], $table['primaryKey']);
        $data = $this->client->getTableDataPreview(getenv('KBC_TEST_BUCKET') . '.some-table-7');
        $data = explode("\n", $data);
        sort($data);
        self::assertEquals(
            [
                '',
                '"1","Bar"',
                '"1","a"',
                '"4","b"',
                '"5","c"',
                '"id","name"',
            ],
            $data
        );
    }
}
