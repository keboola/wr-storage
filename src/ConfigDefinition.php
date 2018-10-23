<?php

declare(strict_types=1);

namespace Keboola\StorageWriter;

use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

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
                ->scalarNode('#token')->isRequired()->cannotBeEmpty()->end()
                ->scalarNode('url')->isRequired()->cannotBeEmpty()->end()
                ->booleanNode('fullSync')->defaultFalse()->end()
                ->booleanNode('incremental')->defaultFalse()->end()
            ->end()
        ;
        // @formatter:on
        return $parametersNode;
    }
}
