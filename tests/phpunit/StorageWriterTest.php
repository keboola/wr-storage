<?php

declare(strict_types=1);

namespace Keboola\StorageWriter\Tests;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageWriter\Component;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Filesystem\Filesystem;

class StorageWriterTest extends TestCase
{
    /**
     * @var Client
     */
    private $client;

    public function setUp() : void
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

    public function testBasic() : void
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
                'bucket' => getenv('KBC_TEST_BUCKET'),
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
        $app = new Component();
        $app->run();
        self::assertTrue($this->client->tableExists(getenv('KBC_TEST_BUCKET') . '.some-table-1'));
    }

    public function testAlreadyExists() : void
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
                'bucket' => getenv('KBC_TEST_BUCKET'),
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
        $app = new Component();
        $app->run();
        self::assertTrue($this->client->tableExists(getenv('KBC_TEST_BUCKET') . '.some-table-2'));
    }
}
