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
    private const ACTION_RUN = 'run';

    public function run(): void
    {
        try {
            /** @var Config $config */
            $config = $this->getConfig();
            $client = new Client(['token' => $config->getToken(), 'url' => $config->getUrl()]);
            $authorization = new Authorization($client);
            $bucket = $authorization->getAuthorizedBucket();
            if ($config->getAction() === self::ACTION_RUN) {
                $this->getLogger()->info(
                    sprintf(
                        'Authorized for project "%s" (%s)',
                        $authorization->getAuthorizedProjectName(),
                        $authorization->getAuthorizedProjectId()
                    )
                );
                $this->write($client, $config, $bucket);
            } elseif ($config->getAction() === self::ACTION_INFO) {
                echo \GuzzleHttp\json_encode([
                    'projectId' => $authorization->getAuthorizedProjectId(),
                    'projectName' => $authorization->getAuthorizedProjectName(),
                    'bucket' => $bucket,
                ]);
            } else {
                throw new UserException(sprintf('Unknown action "%s".', $config->getAction()));
            }
        } catch (ClientException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }
    }

    private function write(Client $client, Config $config, string $bucket) : void
    {
        foreach ($config->getInputTables() as $table) {
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
                        \GuzzleHttp\json_encode($tableInfo['primaryKey']),
                        \GuzzleHttp\json_encode($primaryKey)
                    ));
                }
            }
            $client->writeTableAsync($tableId, $csv, ['incremental' => $config->getMode() === 'update']);
            $this->getLogger()->info(sprintf('Table "%s" processed.', $table['destination']));
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
