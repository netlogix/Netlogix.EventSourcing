<?php

declare(strict_types=1);

namespace Netlogix\EventSourcing\Helper;

use Neos\EventSourcing\Event\DomainEventInterface;
use Neos\EventSourcing\EventStore\StreamName;
use Neos\Flow\Annotations as Flow;
use Netlogix\EventSourcing\ValueObject\EventStoreIdentifier;
use RuntimeException;

#[Flow\Proxy(false)]
final class EventsOnEventStoreStream
{
    /**
     * @var DomainEventInterface[]
     */
    private array $events;

    private ?StreamName $streamName = null;

    private ?EventStoreIdentifier $eventStoreIdentifier = null;

    private function __construct(DomainEventInterface ...$events)
    {
        $this->events = $events;
    }

    public static function forEvents(DomainEventInterface ...$events): EventsOnEventStoreStream
    {
        return new static(...$events);
    }

    public function onStream(StreamName $streamName): EventsOnEventStoreStream
    {
        if ($this->streamName !== null) {
            throw new RuntimeException('StreamName was already set!', 1615458800);
        }

        $this->streamName = $streamName;

        return $this;
    }

    public function onEventStore(EventStoreIdentifier $eventStoreIdentifier): EventsOnEventStoreStream
    {
        if ($this->eventStoreIdentifier !== null) {
            throw new RuntimeException('EventStore was already set!', 1615459900);
        }

        $this->eventStoreIdentifier = $eventStoreIdentifier;

        return $this;
    }

    /**
     * @return array|DomainEventInterface[]
     * @internal
     */
    public function getEvents(): array
    {
        return $this->events;
    }

    /**
     * @return StreamName|null
     * @internal
     */
    public function getStreamName(): ?StreamName
    {
        return $this->streamName;
    }

    /**
     * @return EventStoreIdentifier|null
     * @internal
     */
    public function getEventStoreIdentifier(): ?EventStoreIdentifier
    {
        return $this->eventStoreIdentifier;
    }
}
