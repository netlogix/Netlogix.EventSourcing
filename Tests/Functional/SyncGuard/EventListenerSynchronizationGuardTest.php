<?php

declare(strict_types=1);

namespace Netlogix\EventSourcing\Tests\Functional\SyncGuard;

use Doctrine\DBAL\Connection;
use Doctrine\ORM\EntityManagerInterface;
use Neos\Flow\Tests\FunctionalTestCase;
use Neos\EventSourcing\EventListener\AppliedEventsStorage\AppliedEventsLog;
use Neos\EventSourcing\EventListener\EventListenerInterface;
use Neos\EventSourcing\EventStore\StreamName;
use Netlogix\EventSourcing\SyncGuard\EventListenerSynchronizationGuard;
use Netlogix\EventSourcing\SyncGuard\TimeoutWhileWaitingForListener;
use Netlogix\EventSourcing\TestEssentials\EventStore\EventStoreBuilder;
use Netlogix\EventSourcing\Tests\Functional\Fixtures\Domain\Listener\EventListener;
use Netlogix\EventSourcing\Tests\Functional\Fixtures\Domain\Listener\SomethingHappened;
use Netlogix\EventSourcing\ValueObject\EventStoreIdentifier;

class EventListenerSynchronizationGuardTest extends FunctionalTestCase
{
    protected Connection $connection;

    public function setUp(): void
    {
        parent::setUp();

        $this->eventStore = EventStoreBuilder::setupEventStore(
            $this->objectManager,
            self::eventStoreIdentifier()
        );

        $entityManager = $this->objectManager->get(EntityManagerInterface::class);
        $this->connection = $entityManager->getConnection();
        $this->connection->executeStatement('TRUNCATE TABLE ' . AppliedEventsLog::TABLE_NAME);
    }

    /**
     * @test
     */
    public function an_empty_event_stream_is_done(): void
    {
        $eventListener = new EventListener();
        $streamName = StreamName::fromString('My.Stream');

        $retries = EventListenerSynchronizationGuard::forNotStreamAwareListener($eventListener, $streamName)
            ->blockUntilProjectionsAreUpToDate();

        self::assertEquals(0, $retries);
    }

    /**
     * This only makes sure that the test is set up correctly
     *
     * @test
     */
    public function the_fixture_listener_consumes_the_test_event(): void
    {
        $eventListener = new EventListener();
        $streamName = StreamName::fromString('My.Stream');

        EventStoreBuilder::buildEventStoreWithEvents(
            $this->objectManager,
            self::eventStoreIdentifier(),
            $streamName,
            new SomethingHappened('10000'),
            new SomethingHappened('20000')
        );

        $highestAppliedSequenceNumber = $this->fetchHighestAppliedSequenceNumber($eventListener);
        self::assertEquals(2, $highestAppliedSequenceNumber);
    }

    /**
     * This only makes sure that the test is set up correctly
     *
     * @test
     */
    public function the_highestappliedsequencenumber_can_be_reset(): void
    {
        $eventListener = new EventListener();
        $streamName = StreamName::fromString('My.Stream');

        EventStoreBuilder::buildEventStoreWithEvents(
            $this->objectManager,
            self::eventStoreIdentifier(),
            $streamName,
            new SomethingHappened('10000'),
            new SomethingHappened('20000')
        );
        $this->resetHighestAppliedSequenceNumber($eventListener);

        $highestAppliedSequenceNumber = $this->fetchHighestAppliedSequenceNumber($eventListener);
        self::assertEquals(0, $highestAppliedSequenceNumber);
    }

    /**
     * @test
     */
    public function the_sync_guard_will_wait(): void
    {
        $eventListener = new EventListener();
        $streamName = StreamName::fromString('My.Stream');

        EventStoreBuilder::buildEventStoreWithEvents(
            $this->objectManager,
            self::eventStoreIdentifier(),
            $streamName,
            new SomethingHappened('10000'),
            new SomethingHappened('20000')
        );
        $this->resetHighestAppliedSequenceNumber($eventListener);

        $this->expectException(TimeoutWhileWaitingForListener::class);

        EventListenerSynchronizationGuard::forNotStreamAwareListener($eventListener, $streamName)
            ->withoutInvocation()
            ->blockUntilProjectionsAreUpToDate(5);
    }

    protected function resetHighestAppliedSequenceNumber(EventListenerInterface $listener, int $targetValue = 0): void
    {
        $query = $this
            ->connection
            ->createQueryBuilder();

        $query
            ->update(AppliedEventsLog::TABLE_NAME)
            ->set('highestAppliedSequenceNumber', $targetValue)
            ->where(
                $query->expr()->eq(
                    'eventListenerIdentifier',
                    $query->createNamedParameter(get_class($listener))
                )
            )
            ->execute();
    }

    private function fetchHighestAppliedSequenceNumber(EventListenerInterface $listener): int
    {
        $query = $this
            ->connection
            ->createQueryBuilder();

        $result = $query
            ->select('highestappliedsequencenumber')
            ->from(AppliedEventsLog::TABLE_NAME)
            ->where(
                $query->expr()->eq(
                    'eventListenerIdentifier',
                    $query->createNamedParameter(get_class($listener))
                )
            )
            ->execute()
            ->fetchOne();

        return (int)$result;
    }

    private static function eventStoreIdentifier(): EventStoreIdentifier
    {
        return EventStoreIdentifier::fromString('Netlogix.EventSourcing:TestingEventStore');
    }
}
