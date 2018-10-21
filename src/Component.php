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
            $authorization = new Authorization($client);
            $bucket = $authorization->getAuthorizedBucket();
            if ($config->getAction() === 'run') {
                $this->write($client, $config, $bucket);
            } elseif ($config->getAction() === 'targetInfo') {
                echo \GuzzleHttp\json_encode([
                    'project' => $this->getProjectInfo($client),
                    'bucket' => $bucket,
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
                        \GuzzleHttp\json_encode($tableInfo['primaryKey']) .
                        ' does not match the primary key in the source table: ' . \GuzzleHttp\json_encode($primaryKey)
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

    private function getProjectInfo(Client $client): array
    {
        $tokenInfo = $client->verifyToken();
        return [
            'name' => $tokenInfo['owner']['name'],
            'id' => $tokenInfo['owner']['id'],
        ];
    }
}
