<?php

declare(strict_types=1);

namespace Keboola\StorageWriter;

use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ConfigDefinition extends BaseConfigDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();
        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $parametersNode
            ->children()
                ->scalarNode('bucket')->end() // this is only for compatibility with legacy configs and is ignored
                ->scalarNode('#token')->cannotBeEmpty()->end()
                ->scalarNode('url')->cannotBeEmpty()->end()
                ->booleanNode('incremental')->end()
                ->scalarNode('mode')
                    ->defaultValue(Config::MODE_REPLACE)
                    ->validate()
                        ->ifNotInArray([Config::MODE_RECREATE, Config::MODE_REPLACE, Config::MODE_UPDATE])
                        ->thenInvalid(sprintf(
                            'Mode must be one of "%s"',
                            implode(', ', [Config::MODE_RECREATE, Config::MODE_REPLACE, Config::MODE_UPDATE])
                        ))
                    ->end()
                ->end()
            ->end()
        ;
        // @formatter:on
        return $parametersNode;
    }
}
