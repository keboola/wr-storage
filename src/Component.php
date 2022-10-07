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
            if ($config->getAllowSourceProjectId()) {
                $sourceClient = new Client(['token' => $config->getSourceToken(), 'url' => $config->getSourceUrl()]);
                $verifyToken = $sourceClient->verifyToken();
                if ($verifyToken['owner']['id'] !== $config->getAllowSourceProjectId()) {
                    throw new UserException(
                        sprintf('You cannot run the App on this project "%s".', $verifyToken['owner']['name'])
                    );
                }
            }

            $this->getLogger()->info(substr($config->getToken(), 0, 20));
            $this->getLogger()->info($config->getUrl());
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
        foreach ($config->getInputTables($this->getDataDir()) as $table) {
            $this->getLogger()->info(sprintf('Processing table "%s".', $table['destination']));
            $manifest = $this->getManifestManager()->getTableManifest($table['destination']);
            $primaryKey = $manifest['primary_key'] ?? [];
            $csv = new CsvFile($this->getDataDir() . '/in/tables/' . $table['destination']);
            $tableId = $bucket . '.' . $table['destination'];
            if ($config->getMode() === Config::MODE_RECREATE) {
                try {
                    $client->dropTable($tableId);
                } catch (ClientException $e) {
                    if ($e->getCode() !== 404) {
                        throw $e;
                    }
                }
            }
            if (!$client->tableExists($tableId)) {
                $client->createTableAsync(
                    $bucket,
                    basename($table['destination'], '.csv'),
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
