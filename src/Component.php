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
    private const ACTION_INFO = 'info';

    protected function run(): void
    {
        try {
            /** @var Config $config */
            $config = $this->getConfig();
            $client = new Client(['token' => $config->getToken(), 'url' => $config->getUrl()]);
            $authorization = new Authorization($client);
            $bucket = $authorization->getAuthorizedBucket();
            $this->getLogger()->info(
                sprintf(
                    'Authorized for project "%s" (%s)',
                    $authorization->getAuthorizedProjectName(),
                    $authorization->getAuthorizedProjectId()
                )
            );
            $this->write($client, $config, $bucket);
        } catch (ClientException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }
    }

    public function infoAction(): array
    {
        try {
            /** @var Config $config */
            $config = $this->getConfig();
            $client = new Client(['token' => $config->getToken(), 'url' => $config->getUrl()]);
            $authorization = new Authorization($client);
            $bucket = $authorization->getAuthorizedBucket();
            return [
                'projectId' => $authorization->getAuthorizedProjectId(),
                'projectName' => $authorization->getAuthorizedProjectName(),
                'bucket' => $bucket,
            ];
        } catch (ClientException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }
    }

    private function write(Client $client, Config $config, string $bucket): void
    {
        $tables = $config->getInputTables();
        if (empty($tables)) {
            $tables = [
                [
                    'destination' => $config->getTable(),
                ],
            ];
        }
        $this->getLogger()->info('Show tables');
        $this->getLogger()->info(print_r($tables, true));
        $this->getLogger()->info(print_r($config->getTable(), true));
        foreach ($tables as $table) {
            $this->getLogger()->info(sprintf('Processing table "%s".', $table['destination']));
            $manifest = $this->getManifestManager()->getTableManifest($table['destination'] . '.csv');
            $this->getLogger()->info(print_r($manifest, true));
            $primaryKey = $manifest['primary_key'] ?? [];
            $csv = new CsvFile($this->getDataDir() . '/in/tables/' . $table['destination'] . '.csv');
            $tableId = $bucket . '.' . $table['destination'];
            $this->getLogger()->info('TableId ' . $tableId);
            if ($config->getMode() === Config::MODE_RECREATE) {
                try {
                    $client->dropTable($tableId);
                } catch (ClientException $e) {
                    if ($e->getCode() !== 404) {
                        throw $e;
                    }
                }
            }
            $this->getLogger()->info('Bucket ' . $bucket);
            if (!$client->tableExists($tableId)) {
                $client->createTableAsync(
                    $bucket,
                    $table['destination'],
                    $csv,
                    [
                        'primaryKey' => implode(',', $primaryKey),
                    ]
                );
            } else {
                $tableInfo = $client->getTable($tableId);
                if ($tableInfo['primaryKey'] !== $primaryKey) {
                    throw new UserException(sprintf(
                        'Primary key in the destination table "%s" - %s ' .
                        'does not match the primary key in the source table - %s.',
                        $table['destination'],
                        json_encode($tableInfo['primaryKey']),
                        json_encode($primaryKey)
                    ));
                }
            }
            $client->writeTableAsync($tableId, $csv, ['incremental' => $config->getMode() === 'update']);
            $this->getLogger()->info(sprintf('Table "%s" processed.', $table['destination']));
        }
    }

    protected function getSyncActions(): array
    {
        return [
            self::ACTION_INFO => 'infoAction',
        ];
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
