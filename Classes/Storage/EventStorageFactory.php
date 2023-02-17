<?php

declare(strict_types=1);

namespace Netlogix\EventSourcing\Storage;

use Neos\EventSourcing\EventStore\Exception\StorageConfigurationException;
use Neos\EventSourcing\EventStore\Storage\EventStorageInterface;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\ObjectManagement\ObjectManagerInterface;
use RuntimeException;

#[Flow\Scope('singleton')]
final class EventStorageFactory
{
    #[Flow\Inject]
    protected ObjectManagerInterface $objectManager;

    #[Flow\InjectConfiguration(package: 'Neos.EventSourcing', path: 'EventStore.stores')]
    protected array $eventStores = [];

    /**
     * @var array<string, EventStorageInterface> EventStorageInterface
     */
    protected array $eventStorages = [];

    public function getStorageForEventStore(string $eventStoreName): EventStorageInterface
    {
        if (!isset($this->eventStores[$eventStoreName])) {
            throw new RuntimeException(
                sprintf(
                    'No Event Store with the identifier "%s" is configured',
                    $eventStoreName
                ), 1570534022
            );
        }

        if (isset($this->eventStorages[$eventStoreName])) {
            return $this->eventStorages[$eventStoreName];
        }

        $storageClassName = $this->eventStores[$eventStoreName]['storage'];
        $storageOptions = $this->eventStores[$eventStoreName]['storageOptions'] ?? [];

        $storage = $this->objectManager->get($storageClassName, $storageOptions);
        if (!$storage instanceof EventStorageInterface) {
            throw new StorageConfigurationException(
                sprintf(
                    'The configured Storage for Event Store "%s" does not implement the EventStorageInterface',
                    $eventStoreName
                ), 1570533986
            );
        }

        $this->eventStorages[$eventStoreName] = $storage;
        return $storage;
    }
}
