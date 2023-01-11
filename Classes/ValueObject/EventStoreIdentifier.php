<?php

declare(strict_types=1);

namespace Netlogix\EventSourcing\ValueObject;

use Neos\Flow\Annotations as Flow;
use Stringable;

#[Flow\Proxy(false)]
final readonly class EventStoreIdentifier implements Stringable
{
    private function __construct(
        private string $identifier
    ) {
    }

    public static function fromString(string $identifier): EventStoreIdentifier
    {
        return new EventStoreIdentifier($identifier);
    }

    public function __toString(): string
    {
        return $this->identifier;
    }
}
