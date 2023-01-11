<?php

declare(strict_types=1);

namespace Netlogix\EventSourcing\FastRabbit;

use Flowpack\JobQueue\Common\Exception as JobQueueException;
use Neos\EventSourcing\EventListener\EventListenerInterface;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Aop\JoinPointInterface;
use Neos\Flow\ObjectManagement\ObjectManagerInterface;
use Neos\Flow\Reflection\ReflectionService;

use function class_exists;
use function is_a;
use function is_string;
use function json_decode;
use function json_encode;
use function ltrim;
use function str_replace;
use function substr;

/**
 * @Flow\Aspect
 * @Flow\Scope("singleton")
 */
final class ConfigureIndividualListenerQueuesAspect
{
    protected $template;

    /**
     * @var ObjectManagerInterface
     */
    protected $objectManager;

    public function __construct(ObjectManagerInterface $objectManager)
    {
        $this->objectManager = $objectManager;
    }

    /**
     * @Flow\CompileStatic
     */
    public static function collectEventListenerClassNames(
        ObjectManagerInterface $objectManager
    ): array {
        $reflectionService = $objectManager->get(ReflectionService::class);
        return $reflectionService->getAllImplementationClassNamesForInterface(EventListenerInterface::class);
    }

    /**
     * @Flow\Around("method(public Flowpack\JobQueue\Common\Queue\QueueManager->getQueueSettings())")
     */
    public function introduceIndividualListenerQueues(JoinPointInterface $joinPoint): mixed
    {
        $queueName = $joinPoint->getMethodArgument('queueName');
        assert(is_string($queueName));

        try {
            return $joinPoint->getAdviceChain()->proceed($joinPoint);
        } catch (JobQueueException $exception) {
            if (!class_exists($queueName) || !is_a($queueName, EventListenerInterface::class, true)) {
                throw $exception;
            }
        }

        if (!is_string($this->template)) {
            $joinPoint->setMethodArgument('queueName', EventListenerInterface::class);
            $this->template = json_encode($joinPoint->getAdviceChain()->proceed($joinPoint));
        }

        $className = ltrim($queueName, '\\');

        $template = $this->template;
        $template = str_replace('__CLASS__', substr(json_encode($className), 1, -1), $template);

        return json_decode($template, true, 512);
    }
}
