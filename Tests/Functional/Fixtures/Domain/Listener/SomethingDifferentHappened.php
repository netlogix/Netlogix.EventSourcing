<?php

declare(strict_types=1);

namespace Netlogix\EventSourcing\Tests\Functional\Fixtures\Domain\Listener;

use Neos\EventSourcing\Event\DomainEventInterface;
use Neos\Flow\Annotations as Flow;

#[Flow\Proxy(false)]
final readonly class SomethingDifferentHappened implements DomainEventInterface
{
    public function __construct(public string $bar)
    {
    }
}
