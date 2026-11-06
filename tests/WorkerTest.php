<?php

use tobimori\Queues\JobStatus;
use tobimori\Queues\Queues;
use tobimori\Queues\Tests\Jobs\ProcessDataJob;
use tobimori\Queues\Tests\Jobs\ScheduledTestJob;
use tobimori\Queues\Worker\Worker;

test('worker with stopWhenEmpty processes all jobs and exits', function () {
	$first = queue(ProcessDataJob::class, ['data' => 'one']);
	$second = queue(ProcessDataJob::class, ['data' => 'two']);

	(new Worker(Queues::manager(), null, ['sleep' => 0]))->work(null, false, true);

	expect(ProcessDataJob::$processed)->toBe(['one', 'two']);
	expect(Queues::manager()->storage()->get($first)['status'])->toBe(JobStatus::COMPLETED->value);
	expect(Queues::manager()->storage()->get($second)['status'])->toBe(JobStatus::COMPLETED->value);
});

test('worker with stopWhenEmpty exits immediately on an empty queue', function () {
	$start = microtime(true);

	(new Worker(Queues::manager(), null, ['sleep' => 5]))->work(null, false, true);

	expect(microtime(true) - $start)->toBeLessThan(2);
});

test('worker with stopWhenEmpty queues and runs due scheduled jobs', function () {
	Queues::schedule('* * * * *', ScheduledTestJob::class);

	// make the schedule due now
	$storage = Queues::manager()->storage();
	$scheduled = $storage->getScheduled();
	foreach ($scheduled as &$schedule) {
		$schedule['nextRun'] = time() - 1;
	}
	$storage->saveScheduled($scheduled);
	Queues::init();

	(new Worker(Queues::manager(), null, ['sleep' => 0]))->work(null, false, true);

	expect(Queues::manager()->getByStatus(JobStatus::COMPLETED))->toHaveCount(1);
	expect(Queues::manager()->getByStatus(JobStatus::PENDING))->toHaveCount(0);
});
