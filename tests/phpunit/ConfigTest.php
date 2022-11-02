<?php

declare(strict_types=1);

namespace Keboola\StorageWriter\Tests;

use Generator;
use Keboola\StorageWriter\Config;
use Keboola\StorageWriter\ConfigDefinition;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class ConfigTest extends TestCase
{
    /** @dataProvider imageParametersConfigDataProvider */
    public function testImageParametersConfig(array $configData, array $expectedData): void
    {
        $config = new Config($configData, new ConfigDefinition());

        $data = [
            'url' => $config->getUrl(),
            'token' => $config->getToken(),
        ];

        Assert::assertEquals($expectedData, $data);
    }

    public function imageParametersConfigDataProvider(): Generator
    {
        yield 'empty-image-parameters' => [
            [
                'parameters' => [
                    'url' => 'parametersUrl',
                    '#token' => 'parametersToken',
                ],
            ],
            [
                'url' => 'parametersUrl',
                'token' => 'parametersToken',
            ],
        ];

        yield 'url-image-parameters' => [
            [
                'parameters' => [
                    'url' => 'parametersUrl',
                    '#token' => 'parametersToken',
                ],
                'image_parameters' => [
                    'url' => 'image_parameters_url',
                ],
            ],
            [
                'url' => 'image_parameters_url',
                'token' => 'parametersToken',
            ],
        ];

        yield 'token-image-parameters' => [
            [
                'parameters' => [
                    'url' => 'parametersUrl',
                    '#token' => 'parametersToken',
                ],
                'image_parameters' => [
                    '#token' => 'image_parameters_token',
                ],
            ],
            [
                'url' => 'parametersUrl',
                'token' => 'image_parameters_token',
            ],
        ];

        yield 'url-token-image-parameters' => [
            [
                'parameters' => [
                    'url' => 'parametersUrl',
                    '#token' => 'parametersToken',
                ],
                'image_parameters' => [
                    'url' => 'image_parameters_url',
                    '#token' => 'image_parameters_token',
                ],
            ],
            [
                'url' => 'image_parameters_url',
                'token' => 'image_parameters_token',
            ],
        ];
    }
}
