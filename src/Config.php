<?php

declare(strict_types=1);

namespace Keboola\StorageWriter;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    public function getToken(): string
    {
        return (string) $this->getValue(['parameters', '#token']);
    }

    public function getUrl(): string
    {
        return (string) $this->getValue(['parameters', 'url']);
    }

    public function isIncremental(): bool
    {
        return (bool) $this->getValue(['parameters', 'incremental']);
    }

    public function isFullSync(): bool
    {
        return (bool) $this->getValue(['parameters', 'fullSync']);
    }
}
