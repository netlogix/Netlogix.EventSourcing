<?php

declare(strict_types=1);

namespace Netlogix\EventSourcing\FastRabbit;

use Neos\EventSourcing\EventListener\Mapping\DefaultEventToListenerMappingProvider;
use Neos\EventSourcing\EventPublisher\DeferEventPublisher;
use Neos\EventSourcing\EventPublisher\EventPublisherFactoryInterface;
use Neos\EventSourcing\EventPublisher\EventPublisherInterface;
use Neos\Flow\Annotations as Flow;

/**
 * An Event Publisher factory creating a DeferEventPublisher wrapping a FastRabbitJobQueueEventPublisher
 */
#[Flow\Scope('singleton')]
final class FastRabbitEventPublisherFactory implements EventPublisherFactoryInterface
{
    /**
     * A list of all initialized Event Publisher instances, indexed by the "Event Store identifier"
     *
     * @var EventPublisherInterface[]
     */
    private array $eventPublisherInstances;

    public function __construct(
        private readonly DefaultEventToListenerMappingProvider $mappingProvider
    ) {
    }

    public function create(string $eventStoreIdentifier): EventPublisherInterface
    {
        if (!isset($this->eventPublisherInstances[$eventStoreIdentifier])) {
            $mappings = $this->mappingProvider->getMappingsForEventStore($eventStoreIdentifier);
            $this->eventPublisherInstances[$eventStoreIdentifier] = DeferEventPublisher::forPublisher(
                new FastRabbitJobQueueEventPublisher($eventStoreIdentifier, $mappings)
            );
        }

        return $this->eventPublisherInstances[$eventStoreIdentifier];
    }
}
