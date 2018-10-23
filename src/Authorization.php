<?php

declare(strict_types=1);

namespace Keboola\StorageWriter;

use Keboola\Component\UserException;
use Keboola\StorageApi\Client;

class Authorization
{
    /**
     * @var string
     */
    private $authorizedBucket;

    public function __construct(Client $client)
    {
        $tokenInfo = $client->verifyToken();
        $this->validateNumberOfBuckets($tokenInfo);
        $bucket = (string) array_keys($tokenInfo['bucketPermissions'])[0];
        $this->validateBucketPermissions($tokenInfo, $bucket);
        $this->authorizedBucket = $bucket;
    }

    private function validateNumberOfBuckets(array $tokenInfo): void
    {
        if (count($tokenInfo['bucketPermissions']) !== 1) {
            throw new UserException('The token must have write permissions to a single bucket only.');
        }
    }

    private function validateBucketPermissions(array $tokenInfo, string $bucket): void
    {
        if ($tokenInfo['bucketPermissions'][$bucket] !== 'write') {
            throw new UserException(
                sprintf('The token must have only write permissions to the bucket "%s".', $bucket)
            );
        }
    }

    public function getAuthorizedBucket(): string
    {
        return $this->authorizedBucket;
    }
}
