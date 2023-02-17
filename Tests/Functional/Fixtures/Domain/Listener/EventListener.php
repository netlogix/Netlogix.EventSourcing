<?php

declare(strict_types=1);

namespace Netlogix\EventSourcing\Tests\Functional\Fixtures\Domain\Listener;

use Neos\EventSourcing\EventListener\EventListenerInterface;

class EventListener implements EventListenerInterface
{
    public array $values = [];

    public function whenSomethingHappened(SomethingHappened $event): void
    {
        $this->values[] = $event->foo;
    }
}
