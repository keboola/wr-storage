<?php

declare(strict_types=1);

namespace Keboola\StorageWriter\Tests;

use Keboola\Component\UserException;
use Keboola\StorageApi\Client;
use Keboola\StorageWriter\Authorization;
use PHPUnit\Framework\TestCase;

class AuthorizationTest extends TestCase
{
    private function getClientMock(array $verifyTokenData): Client
    {
        $client = self::getMockBuilder(Client::class)
            ->disableOriginalConstructor()
            ->setMethods(['verifyToken'])
            ->getMock();
        $client->method('verifyToken')
            ->willReturn($verifyTokenData);
        /** @var Client $client */
        return $client;
    }

    public function testValidToken(): void
    {
        $client = $this->getClientMock([
            'bucketPermissions' => ['in.some-bucket' => 'write'],
            'owner' => ['id' => 1, 'name' => 'test'],
        ]);
        $authorization = new Authorization($client);
        self::assertSame('in.some-bucket', $authorization->getAuthorizedBucket());
    }

    /**
     * @dataProvider invalidTokenPermissionsProvider
     */
    public function testInvalidToken(array $permissions, string $error): void
    {
        $client = $this->getClientMock($permissions);
        self::expectExceptionMessage($error);
        self::expectException(UserException::class);
        new Authorization($client);
    }

    public function invalidTokenPermissionsProvider(): array
    {
        return [
            [
                ['bucketPermissions' => ['in.some-bucket' => 'manage']],
                'The token must have only write permissions to the bucket "in.some-bucket".',
            ],
            [
                ['bucketPermissions' => ['in.some-bucket' => 'write', 'out.another-bucket' => 'read']],
                'The token must have write permissions to a single bucket only.',
            ],
            [
                ['bucketPermissions' => []],
                'The token must have write permissions to a single bucket only.',
            ],
        ];
    }
}
