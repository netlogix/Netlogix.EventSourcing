<?php

declare(strict_types=1);

namespace Netlogix\EventSourcing\FastRabbit;

use Flowpack\JobQueue\Common\Job\JobManager;
use function get_class;
use Neos\EventSourcing\Event\DecoratedEvent;
use Neos\EventSourcing\Event\DomainEventInterface;
use Neos\EventSourcing\Event\DomainEvents;
use Neos\EventSourcing\EventListener\Mapping\EventToListenerMappings;
use Neos\EventSourcing\EventPublisher\EventPublisherInterface;
use Neos\EventSourcing\EventPublisher\JobQueue\CatchUpEventListenerJob;
use Neos\Flow\Annotations as Flow;

/**
 * An Event Publisher that uses a Job Queue from the Flowpack.JobQueue package to notify Event Listeners of new Events.
 *
 * It sends a CatchUpEventListenerJob to the configured queue for each individual Event Listener class that is affected
 * (i.e. that is registered for at least one of the published Events).
 *
 * The queue name is "neos-eventsourcing" by default, but that can be changed with the "queueName" option. Other options
 * can be specified via ("queueOptions") – see "Submit options" documentation of the corresponding JobQueue
 * implementation.
 *
 * Example configuration:
 *
 * Neos: EventSourcing: EventStore: stores: 'Some.Package:SomeStore': // ... listeners: 'Some.Package\.*': true
 * queueName: 'custom-queue' queueOptions: priority: 2048
 */
final class FastRabbitJobQueueEventPublisher implements EventPublisherInterface
{
    #[Flow\Inject]
    protected JobManager $jobManager;

    public function __construct(
        private readonly string $eventStoreIdentifier,
        private readonly EventToListenerMappings $mappings
    ) {
    }

    /**
     * Iterate through EventToListenerMappings and queue a CatchUpEventListenerJob for every affected Event Listener.
     *
     * @param DomainEvents $events
     */
    public function publish(DomainEvents $events): void
    {
        $this->publishEvents($events);
    }

    private function publishEvents(DomainEvents $events): void
    {
        if ($events->isEmpty()) {
            return;
        }

        $processedEventClassNames = [];
        $queuedEventListenerClassNames = [];

        foreach ($events as $event) {
            $eventClassName = self::getEventClassName($event);
            // only process every Event type once
            if (array_key_exists($eventClassName, $processedEventClassNames)) {
                continue;
            }
            $processedEventClassNames[$eventClassName] = true;

            foreach ($this->mappings as $mapping) {
                if ($mapping->getEventClassName() !== $eventClassName) {
                    continue;
                }
                // only process every Event Listener once
                if (array_key_exists($mapping->getListenerClassName(), $queuedEventListenerClassNames)) {
                    continue;
                }

                $job = new CatchUpEventListenerJob($mapping->getListenerClassName(), $this->eventStoreIdentifier);
                $queueName = $mapping->getOption('queueName', $mapping->getListenerClassName());
                $options = $mapping->getOption('queueOptions', []);
                $this->jobManager->queue($queueName, $job, $options);
                $queuedEventListenerClassNames[$mapping->getListenerClassName()] = true;
            }
        }
    }

    private static function getEventClassName(DomainEventInterface $event): string
    {
        return get_class($event instanceof DecoratedEvent ? $event->getWrappedEvent() : $event);
    }
}
