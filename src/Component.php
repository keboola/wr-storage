<?php

declare(strict_types=1);

namespace Keboola\StorageWriter;

use Keboola\Component\BaseComponent;
use Keboola\Component\UserException;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;

class Component extends BaseComponent
{
    public function run(): void
    {
        /** @var Config $config */
        $config = $this->getConfig();
        $client = new Client(['token' => $config->getToken(), 'url' => $config->getUrl()]);
        $tokenInfo = $client->verifyToken();
        if (count($tokenInfo['bucketPermissions']) > 1) {
            throw new UserException("The token has too broad permissions.");
        }
        $bucket = array_keys($tokenInfo['bucketPermissions'])[0];
        if ($tokenInfo['bucketPermissions'][$bucket] !== 'write') {
            throw new UserException("The token does not have write permissions to the bucket " . $bucket);
        }
        foreach ($config->getInputTables() as $table) {
            echo "Processing table " . $table['destination'];
            $manifest = $this->getManifestManager()->getTableManifest($table['destination']);
            $csv = new CsvFile($this->getDataDir() . '/in/tables/' . $table['destination']);
            try {
                $tableId = $bucket . '.' . $table['destination'];
                if ($client->tableExists($tableId)) {
                    $client->writeTableAsync($tableId, $csv);
                } else {
                    $client->createTableAsync(
                        $bucket,
                        $table['destination'],
                        $csv,
                        [
                            'primaryKey' => implode(',', $manifest['primary_key']),
                        ]
                    );
                }
            } catch (ClientException $e) {
                throw new UserException($e->getMessage());
            }
        }
        echo "All done.";
    }

    protected function getConfigClass(): string
    {
        return Config::class;
    }

    protected function getConfigDefinitionClass(): string
    {
        return ConfigDefinition::class;
    }
}
