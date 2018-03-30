<?php

declare(strict_types=1);

namespace Keboola\StorageWriter\Tests;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
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
        if (empty(getenv('KBC_TEST_TOKEN')) || empty(getenv('KBC_TEST_URL'))) {
            throw new \Exception("KBC_TEST_TOKEN or KBC_TEST_URL is empty");
        }
        $this->client = new Client([
            'token' => getenv('KBC_TEST_TOKEN'),
            'url' => getenv('KBC_TEST_URL')
        ]);
        $tables = $this->client->listTables('in.c-wr-storage-test');
        foreach ($tables as $table) {
            $this->client->dropTable($table['id']);
        }
    }

    public function tearDown() : void
    {
        parent::tearDown();
    }

    public function testBasic() : void
    {
        $temp = new Temp('wr-storage');
        $temp->initRunFolder();
        $baseDir = $temp->getTmpFolder();
        $fs = new Filesystem();
        $fs->mkdir($baseDir . '/in/tables/');
        $tableName = $baseDir . '/in/tables/some-table';
        $fs->dumpFile($tableName, "\"id\",\"name\"\n\"1\",\"Bar\"\n\"2\",\"Kochba\"\n\"3\",\"Foo\"\n");
        $manifest = [
            'primary_key' => ['id'],
        ];
        $fs->dumpFile($tableName . '.manifest', \GuzzleHttp\json_encode($manifest));
        $configFile = [
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
                'bucket' => 'in.c-wr-storage-test',
            ],
            'storage' => [
                'input' => [
                    'tables' => [
                        [
                            'source' => "in.c-main.some-source",
                            'destination' => 'some-table',
                        ],
                    ],
                ],
            ],
        ];
        $fs->dumpFile($baseDir . '/config.json', \GuzzleHttp\json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component();
        $app->run();
        self::assertTrue($this->client->tableExists('in.c-wr-storage-test.some-table'));
    }
}
