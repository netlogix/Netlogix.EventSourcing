<?php

declare(strict_types=1);

namespace Netlogix\EventSourcing\Helper;

use Neos\EventSourcing\Event\DecoratedEvent;
use Neos\EventSourcing\Event\DomainEventInterface;
use Neos\EventSourcing\Event\DomainEvents;
use Neos\EventSourcing\EventStore\EventStoreFactory;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Utility\Algorithms;
use RuntimeException;

final class EventCommitHelper
{
    #[Flow\Inject(lazy: false)]
    protected EventStoreFactory $eventStoreFactory;

    /**
     * @var EventsOnEventStoreStream[]
     */
    private array $events = [];

    private ?string $correlationId;

    private ?string $causationId;

    private bool $emptyEventsAllowed = false;

    private function __construct()
    {
    }

    public function attach(EventsOnEventStoreStream ...$eventsOnStream): self
    {
        if (count($eventsOnStream) > 0) {
            array_push($this->events, ...$eventsOnStream);
        }

        return $this;
    }

    public function withAutogeneratedCorrelationId(): self
    {
        return $this->withCorrelationId(Algorithms::generateUUID());
    }

    public function withCorrelationId(string $correlationId): self
    {
        if ($this->correlationId !== null) {
            throw new RuntimeException('CorrelationId was already set!', 1594125085);
        }

        $this->correlationId = $correlationId;

        return $this;
    }

    public function withCausationId(string $causationId): self
    {
        if ($this->causationId !== null) {
            throw new RuntimeException('CausationId was already set!', 1594125125);
        }

        $this->causationId = $causationId;

        return $this;
    }

    public function allowEmptyEvents(): self
    {
        $this->emptyEventsAllowed = true;

        return $this;
    }

    public function commit(): void
    {
        if (count($this->events) === 0) {
            if ($this->emptyEventsAllowed) {
                return;
            }

            throw new RuntimeException('Empty Events are disallowed and no events were committed', 1594125257);
        }

        array_walk($this->events, function (EventsOnEventStoreStream $eventsOnStream) {
            if (!$this->emptyEventsAllowed && count($eventsOnStream->getEvents()) === 0) {
                throw new RuntimeException('Empty Events are disallowed and no events were committed', 1594895679);
            }
        });

        foreach ($this->events as $eventsOnStream) {
            $eventStoreIdentifier = $eventsOnStream->getEventStoreIdentifier();
            $streamName = $eventsOnStream->getStreamName();
            $events = $eventsOnStream->getEvents();

            if ($eventStoreIdentifier === null) {
                throw new RuntimeException('No EventStoreIdentifier was set!', 1615459300);
            }
            if ($streamName === null) {
                throw new RuntimeException('No StreamName was set!', 1594125316);
            }

            if ($this->causationId !== null) {
                $events = array_map(
                    fn (DomainEventInterface $event) => DecoratedEvent::addCausationIdentifier(
                        $event,
                        $this->causationId
                    ),
                    $events
                );
            }

            if ($this->correlationId !== null) {
                $events = array_map(
                    fn (DomainEventInterface $event) => DecoratedEvent::addCorrelationIdentifier(
                        $event,
                        $this->correlationId
                    ),
                    $events
                );
            }
            $domainEvents = DomainEvents::fromArray($events);

            $eventStore = $this->eventStoreFactory->create((string) $eventStoreIdentifier);
            $eventStore->commit($streamName, $domainEvents);
        }
    }

    public static function create(): self
    {
        return new static();
    }
}
