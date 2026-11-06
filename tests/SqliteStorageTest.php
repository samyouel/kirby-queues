<?php

use tobimori\Queues\JobStatus;
use tobimori\Queues\Queues;
use tobimori\Queues\Storage\CacheStorage;
use tobimori\Queues\Storage\SqliteStorage;

beforeEach(function () {
	$this->path = sys_get_temp_dir() . '/kirby-queues-' . bin2hex(random_bytes(6)) . '.sqlite';
	$this->storage = new SqliteStorage($this->path);
});

afterEach(function () {
	foreach ([$this->path, $this->path . '-wal', $this->path . '-shm'] as $file) {
		@unlink($file);
	}
});

function sqliteTestJob(string $id, array $data = []): array
{
	return array_merge([
		'id' => $id,
		'type' => 'test',
		'payload' => [],
		'attempts' => 0
	], $data);
}

/**
 * Runs the given commands in parallel and returns their stdout
 *
 * @param array<string> $commands
 * @return array<string>
 */
function runInParallel(array $commands): array
{
	$processes = [];
	foreach ($commands as $command) {
		$pipes = [];
		$processes[] = [proc_open($command, [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes), $pipes];
	}

	$output = [];
	foreach ($processes as [$process, $pipes]) {
		$output[] = stream_get_contents($pipes[1]);
		$error = stream_get_contents($pipes[2]);
		fclose($pipes[1]);
		fclose($pipes[2]);
		expect(proc_close($process))->toBe(0, $error);
	}

	return $output;
}

test('storage option selects the storage implementation', function () {
	expect(Queues::createStorage())->toBeInstanceOf(
		getenv('QUEUES_STORAGE') === 'sqlite' ? SqliteStorage::class : CacheStorage::class
	);
});

test('pushes and reads a job', function () {
	$this->storage->push('default', sqliteTestJob('a', ['payload' => ['ä' => 'ö/ü']]));

	$job = $this->storage->get('a');

	expect($job['queue'])->toBe('default');
	expect($job['status'])->toBe(JobStatus::PENDING->value);
	expect($job['payload'])->toBe(['ä' => 'ö/ü']);
	expect($this->storage->get('missing'))->toBeNull();
});

test('pop returns the oldest available job and reserves it', function () {
	$this->storage->push('default', sqliteTestJob('later', ['available_at' => time() + 60]));
	$this->storage->push('default', sqliteTestJob('first', ['available_at' => time() - 10]));
	$this->storage->push('default', sqliteTestJob('second'));
	$this->storage->push('other', sqliteTestJob('other-queue'));

	expect($this->storage->pop('default')['id'])->toBe('first');
	expect($this->storage->pop('default')['id'])->toBe('second');
	expect($this->storage->pop('default'))->toBeNull();

	expect($this->storage->get('first')['status'])->toBe(JobStatus::RUNNING->value);
	expect($this->storage->get('later')['status'])->toBe(JobStatus::PENDING->value);
});

test('a second connection cannot pop a reserved job', function () {
	$this->storage->push('default', sqliteTestJob('only'));

	$otherWorker = new SqliteStorage($this->path);

	expect($this->storage->pop('default')['id'])->toBe('only');
	expect($otherWorker->pop('default'))->toBeNull();
});

test('job lifecycle updates status and attempts', function () {
	$this->storage->push('default', sqliteTestJob('job'));
	$this->storage->pop('default');

	$this->storage->markRunning('job', 'host:1:abc');
	expect($this->storage->get('job'))
		->status->toBe(JobStatus::RUNNING->value)
		->worker_id->toBe('host:1:abc')
		->attempts->toBe(1);

	$this->storage->release('job', 30);
	$job = $this->storage->get('job');
	expect($job['status'])->toBe(JobStatus::PENDING->value);
	expect($job['worker_id'])->toBeNull();
	expect($job['available_at'])->toBeGreaterThanOrEqual(time() + 29);
	expect($this->storage->pop('default'))->toBeNull();

	$this->storage->markFailed('job', 'Boom', ['class' => 'Exception']);
	expect($this->storage->get('job'))
		->status->toBe(JobStatus::FAILED->value)
		->error->toBe('Boom');
});

test('getByStatus sorts newest first and pages correctly', function () {
	foreach (['a' => 100, 'b' => 300, 'c' => 200] as $id => $createdAt) {
		$this->storage->push('default', sqliteTestJob($id, ['created_at' => $createdAt]));
	}
	$this->storage->markCompleted('c');

	$pending = $this->storage->getByStatus(JobStatus::PENDING->value);
	expect(array_column($pending, 'id'))->toBe(['b', 'a']);
	expect(array_column($this->storage->getByStatus(JobStatus::PENDING->value, 1, 1), 'id'))->toBe(['a']);
});

test('stats and queues count jobs', function () {
	$this->storage->push('default', sqliteTestJob('a'));
	$this->storage->push('default', sqliteTestJob('b'));
	$this->storage->push('high', sqliteTestJob('c'));
	$this->storage->markCompleted('b');

	$stats = $this->storage->stats();

	expect($stats['total'])->toBe(3);
	expect($stats['by_status'][JobStatus::PENDING->value])->toBe(2);
	expect($stats['by_status'][JobStatus::COMPLETED->value])->toBe(1);
	expect($stats['by_queue'])->toBe(['default' => 2, 'high' => 1]);
	expect($this->storage->queues())->toBe(['default' => 1, 'high' => 1]);
});

test('clear removes only old finished jobs', function () {
	$this->storage->push('default', sqliteTestJob('old-completed'));
	$this->storage->push('default', sqliteTestJob('new-completed'));
	$this->storage->push('default', sqliteTestJob('old-failed'));
	$this->storage->push('default', sqliteTestJob('pending'));

	$this->storage->update('old-completed', ['status' => JobStatus::COMPLETED->value, 'completed_at' => time() - 25 * 3600]);
	$this->storage->markCompleted('new-completed');
	$this->storage->update('old-failed', ['status' => JobStatus::FAILED->value, 'failed_at' => time() - 200 * 3600]);

	expect($this->storage->clear(24, 168))->toBe(2);
	expect($this->storage->get('old-completed'))->toBeNull();
	expect($this->storage->get('old-failed'))->toBeNull();
	expect($this->storage->get('new-completed'))->not->toBeNull();
	expect($this->storage->get('pending'))->not->toBeNull();
});

test('stores scheduled jobs and batch payloads', function () {
	$this->storage->saveScheduled(['x' => ['job' => 'test']]);
	expect($this->storage->getScheduled())->toBe(['x' => ['job' => 'test']]);

	expect($this->storage->appendBatchPayload('batch', ['n' => 1]))->toBeNull();
	$this->storage->setBatchJobId('batch', 'trigger');
	expect($this->storage->appendBatchPayload('batch', ['n' => 2]))->toBe('trigger');
	expect($this->storage->flushBatchPayloads('batch'))->toBe([['n' => 1], ['n' => 2]]);
	expect($this->storage->flushBatchPayloads('batch'))->toBe([]);
});

test('concurrent writers lose no jobs', function () {
	$script = escapeshellarg(__DIR__ . '/scripts/concurrent-push.php');
	$path = escapeshellarg($this->path);

	runInParallel(array_map(
		fn ($prefix) => PHP_BINARY . " {$script} {$path} {$prefix} 50",
		['a', 'b', 'c', 'd']
	));

	expect($this->storage->stats()['total'])->toBe(200);
	expect($this->storage->queues())->toBe(['default' => 200]);
});

test('concurrent workers pop every job exactly once', function () {
	for ($i = 0; $i < 200; $i++) {
		$this->storage->push('default', sqliteTestJob("job-{$i}"));
	}

	$script = escapeshellarg(__DIR__ . '/scripts/concurrent-pop.php');
	$path = escapeshellarg($this->path);

	$output = runInParallel(array_fill(0, 4, PHP_BINARY . " {$script} {$path}"));
	$popped = array_filter(explode(PHP_EOL, implode('', $output)));

	expect($popped)->toHaveCount(200);
	expect(array_unique($popped))->toHaveCount(200);
});
