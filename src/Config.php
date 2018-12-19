<?php

declare(strict_types=1);

namespace Keboola\StorageWriter;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    public const MODE_UPDATE = 'update';
    public const MODE_REPLACE = 'replace';
    public const MODE_RECREATE = 'recreate';

    public function getToken(): string
    {
        return (string) $this->getValue(['parameters', '#token']);
    }

    public function getUrl(): string
    {
        return (string) $this->getValue(['parameters', 'url']);
    }

    public function getMode(): string
    {
        // can't pass null as default value, because it represents no default.
        // Abuse 0 to identify that incremental flag is not set at all.
        $incremental = $this->getValue(['parameters', 'incremental'], 0);
        // the legacy incremental flag is replaced by the `mode` parameter
        if ($incremental === 0) {
            return $this->getValue(['parameters', 'mode']);
        } else {
            if ($incremental) {
                return self::MODE_UPDATE;
            } else {
                return self::MODE_REPLACE;
            }
        }
    }
}
