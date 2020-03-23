<?php

namespace Digilist\SnakeDumper\Converter;

class ConcatChainConverter implements ConverterInterface
{

    /**
     * @var ConverterInterface[]
     */
    private $converters;

    /**
     * @param ConverterInterface[] $converters
     */
    public function __construct(array $converters = array())
    {
        $this->converters = $converters;
    }

    /**
     * Adds a converter into the chain.
     *
     * @param ConverterInterface $converter
     *
     * @return $this
     */
    public function addConverter(ConverterInterface $converter)
    {
        $this->converters[] = $converter;
    }

    /**
     * @param string $value
     * @param array  $context
     *
     * @return string
     */
    public function convert($value, array $context = array())
    {
        foreach ($this->converters as $converter) {
            $value .= ' ' . $converter->convert($value, $context);
        }

        return trim($value);
    }
}
