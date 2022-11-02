<?php

declare(strict_types=1);

namespace Keboola\StorageWriter;

use Keboola\Component\Config\BaseConfig;
use Symfony\Component\Finder\Finder;

class Config extends BaseConfig
{
    public const MODE_UPDATE = 'update';
    public const MODE_REPLACE = 'replace';
    public const MODE_RECREATE = 'recreate';

    public function getProjectId(): string
    {
        return (string) getenv('KBC_PROJECTID');
    }

    public function getAllowSourceProjectId(): ?int
    {
        return $this->getImageParameters()['sourceProjectId'] ?? null;
    }

    public function getToken(): string
    {
        return $this->getImageParameters()['#token'] ?? $this->getValue(['parameters', '#token']);
    }

    public function getUrl(): string
    {
        return $this->getImageParameters()['url'] ?? $this->getValue(['parameters', 'url']);
    }

    public function getMode(): string
    {
        // can't pass null as default value, because it represents no default.
        // Abuse 0 to identify that incremental flag is not set at all.
        $incremental = $this->getValue(['parameters', 'incremental'], 0);
        // the legacy incremental flag is replaced by the `mode` parameter
        if ($incremental === 0) {
            return $this->getStringValue(['parameters', 'mode']);
        } else {
            if ($incremental) {
                return self::MODE_UPDATE;
            } else {
                return self::MODE_REPLACE;
            }
        }
    }

    public function getInputTables(string $datadir = ''): array
    {
        $tables = $this->getArrayValue(['storage', 'input', 'tables'], []);
        if (empty($tables)) {
            $finder = new Finder();
            $dataFiles = $finder->in($datadir . '/in/tables/')->files()->notName('/\.manifest$/');
            $tables = [];
            foreach ($dataFiles as $dataFile) {
                $tables[] = [
                    'destination' => $dataFile->getFilename(),
                ];
            }
        }
        return $tables;
    }
}
