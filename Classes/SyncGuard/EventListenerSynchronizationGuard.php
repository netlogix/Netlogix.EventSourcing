<?php

declare(strict_types=1);

namespace Netlogix\EventSourcing\SyncGuard;

use Doctrine\DBAL\Connection;
use Doctrine\ORM\EntityManagerInterface;
use Flowpack\JobQueue\Common\Job\JobManager;
use InvalidArgumentException;
use Neos\EventSourcing\EventListener\AppliedEventsStorage\AppliedEventsLog;
use Neos\EventSourcing\EventListener\EventListenerInterface;
use Neos\EventSourcing\EventListener\Mapping\DefaultEventToListenerMappingProvider;
use Neos\EventSourcing\EventPublisher\JobQueue\CatchUpEventListenerJob;
use Neos\EventSourcing\EventStore\Storage\Doctrine\DoctrineEventStorage;
use Neos\EventSourcing\EventStore\StreamAwareEventListenerInterface;
use Neos\EventSourcing\EventStore\StreamName;
use Neos\Flow\Annotations as Flow;
use Netlogix\EventSourcing\Storage\EventStorageFactory;

use function get_class;

final class EventListenerSynchronizationGuard
{
    public const TIMEOUT = 15;

    #[Flow\Inject]
    protected DefaultEventToListenerMappingProvider $eventToListenerMappingProvider;

    #[Flow\Inject]
    protected EntityManagerInterface $entityManager;

    #[Flow\Inject]
    protected JobManager $jobManager;

    #[Flow\Inject]
    protected EventStorageFactory $eventStorageFactory;

    protected EventListenerInterface $listener;

    protected StreamName $streamName;

    /**
     * @var array<string>
     */
    protected array $filter = [];

    protected bool $triggerInvocation = true;

    protected float $waitUntil = 0;

    protected Connection $dbal;

    public function initializeObject(): void
    {
        $this->dbal = $this->entityManager->getConnection();
    }

    protected function __construct(
        EventListenerInterface $listener,
        StreamName $streamName,
        array $filter,
        bool $triggerInvocation,
    ) {
        $this->listener = $listener;
        $this->streamName = $streamName;
        $this->filter = $filter;
        $this->triggerInvocation = $triggerInvocation;
    }

    public function withoutInvocation(): EventListenerSynchronizationGuard
    {
        return new EventListenerSynchronizationGuard(
            $this->listener,
            $this->streamName,
            $this->filter,
            false,
        );
    }

    public function blockUntilProjectionsAreUpToDate(
        int $timeoutInSeconds = self::TIMEOUT,
    ): int {
        $this->invokeEventListener();
        return $this->waitForEventStreamToCatchUpTo($timeoutInSeconds);
    }

    /**
     * We just wait for the given event stream to be up to date, which means
     * according to the highest applied sequence number there are no new events
     * remaining on the stream.
     *
     * @param int $timeoutInSeconds
     */
    private function waitForEventStreamToCatchUpTo(
        int $timeoutInSeconds
    ): int {
        $retries = 0;
        $start = microtime(true);
        $this->waitUntil = $start + $timeoutInSeconds;

        while (true) {
            $now = microtime(true);
            if ($now >= $this->waitUntil) {
                throw new TimeoutWhileWaitingForListener(
                    sprintf(
                        'TIMEOUT while waiting %d seconds for "%s" (%s to %s)',
                        $timeoutInSeconds,
                        get_class($this->listener),
                        date('H:i:s', (int) $start),
                        date('H:i:s', (int) $now)
                    ),
                    1676639978
                );
            }

            if ($this->eventStreamHasCaughtUp()) {
                return $retries;
            }
            $retries++;
            usleep(50000); // 50000μs = 50ms
        }
    }

    protected function eventStreamHasCaughtUp(): bool
    {
        $storage = $this->eventStorageFactory->getStorageForEventStore(
            $this->getEventStoreIdentifierForListener()
        );
        $highestAppliedSequenceNumber = $this->getHighestAppliedSequenceNumber();
        $minimumSequenceNumber = $highestAppliedSequenceNumber + 1;

        assert($storage instanceof DoctrineEventStorage);
        $eventStream = $storage->load($this->streamName, $minimumSequenceNumber);
        $eventStream->rewind();

        return !$eventStream->valid();
    }

    private function getHighestAppliedSequenceNumber(): int
    {
        $listenerClassName = get_class($this->listener);

        $highestAppliedSequenceNumber = $this->dbal->fetchOne(
            '
                SELECT highestAppliedSequenceNumber
                FROM ' . $this->dbal->quoteIdentifier(AppliedEventsLog::TABLE_NAME) . '
                WHERE eventlisteneridentifier = :eventListenerIdentifier '
            . $this->dbal->getDatabasePlatform()->getForUpdateSQL(),
            ['eventListenerIdentifier' => $listenerClassName]
        );

        return $highestAppliedSequenceNumber !== false ? (int) $highestAppliedSequenceNumber : -1;
    }

    private function invokeEventListener(): void
    {
        if (!$this->triggerInvocation) {
            return;
        }
        $listenerClassName = get_class($this->listener);
        $job = new CatchUpEventListenerJob(
            $listenerClassName,
            $this->getEventStoreIdentifierForListener()
        );
        $this->jobManager->queue($listenerClassName, $job);
    }

    private function getEventStoreIdentifierForListener(): string
    {
        return $this->eventToListenerMappingProvider->getEventStoreIdentifierForListenerClassName(
            get_class($this->listener)
        );
    }

    /**
     * @param EventListenerInterface & StreamAwareEventListenerInterface $listener
     * @return EventListenerSynchronizationGuard
     */
    public static function forStreamAwareListener(StreamAwareEventListenerInterface $listener
    ): EventListenerSynchronizationGuard {
        assert($listener instanceof EventListenerInterface);
        return new EventListenerSynchronizationGuard(
            $listener,
            $listener::listensToStream(),
            [],
            true,
        );
    }

    /**
     * @param EventListenerInterface $listener
     * @param StreamName $streamName
     * @return EventListenerSynchronizationGuard
     */
    public static function forNotStreamAwareListener(
        EventListenerInterface $listener,
        StreamName $streamName
    ): EventListenerSynchronizationGuard {
        if ($listener instanceof StreamAwareEventListenerInterface) {
            throw new InvalidArgumentException(
                sprintf('Expected non stream aware listener, stream aware listener (%s) given', get_class($listener)),
                1676631584
            );
        }
        return new EventListenerSynchronizationGuard(
            $listener,
            $streamName,
            [],
            true,
        );
    }

}
