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
        try {
            /** @var Config $config */
            $config = $this->getConfig();
            $client = new Client(['token' => $config->getToken(), 'url' => $config->getUrl()]);
            $tokenInfo = $client->verifyToken();
            if (count($tokenInfo['bucketPermissions']) > 1) {
                throw new UserException('The token has too broad permissions.');
            }
            $bucket = array_keys($tokenInfo['bucketPermissions'])[0];
            if ($tokenInfo['bucketPermissions'][$bucket] !== 'write') {
                throw new UserException('The token does not have write permissions to the bucket ' . $bucket);
            }
            if ($config->getAction() === 'run') {
                $this->write($client, $config, $bucket);
            } elseif ($config->getAction() === 'info') {
                echo \GuzzleHttp\json_encode([
                    'bucket' => $bucket,
                    'projectId' => $tokenInfo['owner']['id'],
                    'projectName' => $tokenInfo['owner']['name'],
                ]);
            } else {
                throw new UserException("Unknown action " . $config->getAction());
            }
        } catch (ClientException $e) {
            throw new UserException($e->getMessage());
        }
    }

    private function write(Client $client, Config $config, string $bucket) : void
    {
        foreach ($config->getInputTables() as $table) {
                $this->getLogger()->info('Processing table ' . $table['destination']);
            $manifest = $this->getManifestManager()->getTableManifest($table['destination']);
            $primaryKey = $manifest['primary_key'] ?? [];
            $csv = new CsvFile($this->getDataDir() . '/in/tables/' . $table['destination']);
            $tableId = $bucket . '.' . $table['destination'];
            if ($client->tableExists($tableId)) {
                    $tableInfo = $client->getTable($tableId);
                    if ($tableInfo['primaryKey'] != $primaryKey) {
                        throw new UserException(
                            'Primary in the destination table ' . $table['destination'] . ' ' .
                            json_encode($tableInfo['primaryKey']) .
                            ' does not match the primary key in the source table: ' . json_encode($primaryKey)
                        );
                    }
                $client->writeTableAsync($tableId, $csv, ['incremental' => $config->isIncremental()]);
            } else {
                $client->createTableAsync(
                    $bucket,
                    $table['destination'],
                    $csv,
                    [
                        'primaryKey' => implode(',', $primaryKey),
                    ]
                );
            }
                $this->getLogger()->info('Table ' . $table['destination'] . ' processed.');
        }
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
