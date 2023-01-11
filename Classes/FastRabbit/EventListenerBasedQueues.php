<?php

declare(strict_types=1);

namespace Netlogix\EventSourcing\FastRabbit;

use Neos\EventSourcing\EventListener\EventListenerInterface;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\ObjectManagement\ObjectManagerInterface;
use Neos\Flow\Reflection\ReflectionService;
use Netlogix\JobQueue\FastRabbit\Queues\Locator;

class EventListenerBasedQueues implements Locator
{
    /**
     * @var \ArrayIterator
     */
    protected $queues;

    public function next()
    {
        $this->queues->next();
    }

    public function key()
    {
        return $this->queues->key();
    }

    public function valid()
    {
        return $this->queues->valid();
    }

    public function rewind()
    {
        $this->queues->rewind();
    }

    public function current(): string
    {
        return $this->queues->current();
    }

    public function injectObjectManager(ObjectManagerInterface $objectManager): void
    {
        $this->queues = new \ArrayIterator(
            self::collectEventListenerClassNames($objectManager)
        );
    }

    /**
     * @Flow\CompileStatic
     * @param ObjectManagerInterface $objectManager
     * @return array
     */
    public static function collectEventListenerClassNames(
        ObjectManagerInterface $objectManager
    ): array {
        $reflectionService = $objectManager->get(ReflectionService::class);
        return $reflectionService->getAllImplementationClassNamesForInterface(EventListenerInterface::class);
    }
}
