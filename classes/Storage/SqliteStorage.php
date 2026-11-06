<?php

namespace tobimori\Queues\Storage;

use Kirby\Cms\App;
use Kirby\Filesystem\Dir;
use PDO;
use tobimori\Queues\JobStatus;

/**
 * SQLite-based storage implementation
 *
 * Stores jobs in a single SQLite database file. Every read-modify-write runs
 * inside an immediate transaction, so concurrent web requests and workers can
 * push, pop and update jobs without losing or duplicating them. Unlike the
 * cache storage, the database path doesn't depend on the request host, so the
 * Panel and CLI workers always share the same jobs.
 *
 * Requires the pdo_sqlite extension and a local filesystem (no network shares,
 * as SQLite relies on file locks).
 *
 * @package tobimori\Queues\Storage
 */
class SqliteStorage implements StorageInterface
{
	/**
	 * @var PDO Database connection
	 */
	protected PDO $db;

	/**
	 * Constructor
	 *
	 * @param string|null $path Path to the database file (defaults to the `tobimori.queues.sqlite.path` option)
	 */
	public function __construct(?string $path = null)
	{
		$path ??= static::defaultPath();

		Dir::make(dirname($path));

		$this->db = new PDO('sqlite:' . $path, null, null, [
			PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
			PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
			// wait for locks held by other processes instead of failing immediately
			PDO::ATTR_TIMEOUT => 10
		]);

		// WAL lets readers (e.g. the Panel) run while a worker writes
		$this->db->exec('PRAGMA journal_mode = WAL');
		$this->db->exec('PRAGMA synchronous = NORMAL');
		$this->db->exec('PRAGMA busy_timeout = 10000');

		$this->migrate();
	}

	/**
	 * Default database path
	 */
	public static function defaultPath(): string
	{
		$kirby = App::instance();

		return $kirby->option('tobimori.queues.sqlite.path')
			?? $kirby->root('cache') . '/tobimori.queues/queues.sqlite';
	}

	/**
	 * @inheritDoc
	 *
	 * @param array<string, mixed> $job Job data
	 */
	public function push(string $queue, array $job): void
	{
		$job += [
			'queue' => $queue,
			'status' => JobStatus::PENDING->value,
			'created_at' => time(),
			'available_at' => time()
		];

		$this->write($job);
	}

	/**
	 * @inheritDoc
	 *
	 * Reserves the job by marking it as running inside the same transaction,
	 * so no other worker can pop it.
	 *
	 * @return array<string, mixed>|null Job data or null if no job available
	 */
	public function pop(string $queue): ?array
	{
		return $this->transaction(function () use ($queue) {
			$statement = $this->db->prepare(
				'SELECT data FROM jobs
				WHERE queue = :queue AND status = :status AND available_at <= :now
				ORDER BY available_at, created_at, rowid
				LIMIT 1'
			);
			$statement->execute([
				'queue' => $queue,
				'status' => JobStatus::PENDING->value,
				'now' => time()
			]);

			$data = $statement->fetchColumn();

			if (!is_string($data)) {
				return null;
			}

			$job = $this->decode($data);

			// a reserved job counts as running from now on; markRunning() adds the worker
			return $this->write(array_merge($job, [
				'status' => JobStatus::RUNNING->value,
				'started_at' => time(),
				'updated_at' => time()
			]));
		});
	}

	/**
	 * @inheritDoc
	 *
	 * @return array<string, mixed>|null Job data or null if not found
	 */
	public function get(string $id): ?array
	{
		$statement = $this->db->prepare('SELECT data FROM jobs WHERE id = :id');
		$statement->execute(['id' => $id]);

		$data = $statement->fetchColumn();

		return is_string($data) ? $this->decode($data) : null;
	}

	/**
	 * @inheritDoc
	 *
	 * @param array<string, mixed> $data Job data to update
	 */
	public function update(string $id, array $data): void
	{
		$this->modify($id, fn (array $job) => array_merge($job, $data));
	}

	/**
	 * @inheritDoc
	 */
	public function delete(string $id): void
	{
		$this->db->prepare('DELETE FROM jobs WHERE id = :id')->execute(['id' => $id]);
	}

	/**
	 * @inheritDoc
	 *
	 * @return array<array<string, mixed>> Array of job data, newest first
	 */
	public function getByStatus(string $status, int $limit = 100, int $offset = 0): array
	{
		$statement = $this->db->prepare(
			'SELECT data FROM jobs
			WHERE status = :status
			ORDER BY created_at DESC, rowid DESC
			LIMIT :limit OFFSET :offset'
		);
		$statement->bindValue('status', $status);
		$statement->bindValue('limit', $limit, PDO::PARAM_INT);
		$statement->bindValue('offset', $offset, PDO::PARAM_INT);
		$statement->execute();

		return array_map(
			fn (string $data) => $this->decode($data),
			$statement->fetchAll(PDO::FETCH_COLUMN)
		);
	}

	/**
	 * @inheritDoc
	 *
	 * @return array<string, mixed> Statistics data
	 */
	public function stats(): array
	{
		$stats = [
			'total' => 0,
			'by_status' => array_fill_keys(array_map(fn (JobStatus $status) => $status->value, JobStatus::cases()), 0),
			'by_queue' => []
		];

		$statement = $this->db->prepare('SELECT queue, status, COUNT(*) AS count FROM jobs GROUP BY queue, status');
		$statement->execute();

		foreach ($statement as $row) {
			$count = (int)$row['count'];
			$stats['total'] += $count;
			$stats['by_status'][$row['status']] = ($stats['by_status'][$row['status']] ?? 0) + $count;
			$stats['by_queue'][$row['queue']] = ($stats['by_queue'][$row['queue']] ?? 0) + $count;
		}

		return $stats;
	}

	/**
	 * @inheritDoc
	 *
	 * @return array<string, int> Queue names mapped to the number of pending jobs
	 */
	public function queues(): array
	{
		$statement = $this->db->prepare(
			'SELECT queue, SUM(status = :status) AS count FROM jobs GROUP BY queue ORDER BY queue'
		);
		$statement->execute(['status' => JobStatus::PENDING->value]);

		$queues = [];
		foreach ($statement as $row) {
			$queues[$row['queue']] = (int)$row['count'];
		}

		return $queues;
	}

	/**
	 * @inheritDoc
	 */
	public function clear(int $completedHours = 24, int $failedHours = 168): int
	{
		$now = time();
		$statement = $this->db->prepare(
			'DELETE FROM jobs
			WHERE (status = :completed AND finished_at < :completedCutoff)
			OR (status = :failed AND finished_at < :failedCutoff)'
		);
		$statement->execute([
			'completed' => JobStatus::COMPLETED->value,
			'completedCutoff' => $now - ($completedHours * 3600),
			'failed' => JobStatus::FAILED->value,
			'failedCutoff' => $now - ($failedHours * 3600)
		]);

		return $statement->rowCount();
	}

	/**
	 * @inheritDoc
	 *
	 * @return array<string, mixed> Scheduled jobs data
	 */
	public function getScheduled(): array
	{
		return $this->getValue('scheduled') ?? [];
	}

	/**
	 * @inheritDoc
	 *
	 * @param array<string, mixed> $scheduled Scheduled jobs data
	 */
	public function saveScheduled(array $scheduled): void
	{
		$this->setValue('scheduled', $scheduled);
	}

	/**
	 * @inheritDoc
	 */
	public function markRunning(string $id, string $workerId): void
	{
		$this->modify($id, fn (array $job) => array_merge($job, [
			'status' => JobStatus::RUNNING->value,
			'started_at' => time(),
			'worker_id' => $workerId,
			'attempts' => ($job['attempts'] ?? 0) + 1
		]));
	}

	/**
	 * @inheritDoc
	 */
	public function markCompleted(string $id, mixed $result = null): void
	{
		$this->update($id, [
			'status' => JobStatus::COMPLETED->value,
			'completed_at' => time(),
			'result' => $result
		]);
	}

	/**
	 * @inheritDoc
	 *
	 * @param array<string, mixed> $exception Exception data
	 */
	public function markFailed(string $id, string $error, array $exception = []): void
	{
		$this->update($id, [
			'status' => JobStatus::FAILED->value,
			'failed_at' => time(),
			'error' => $error,
			'exception' => $exception
		]);
	}

	/**
	 * @inheritDoc
	 */
	public function release(string $id, int $delay = 0): void
	{
		$this->update($id, [
			'status' => JobStatus::PENDING->value,
			'available_at' => time() + $delay,
			'worker_id' => null
		]);
	}

	/** @inheritDoc */
	public function appendBatchPayload(string $batchKey, array $payload): ?string
	{
		return $this->transaction(function () use ($batchKey, $payload) {
			$batch = $this->getValue("batch.{$batchKey}") ?? ['jobId' => null, 'payloads' => []];
			$batch['payloads'][] = $payload;
			$this->setValue("batch.{$batchKey}", $batch);

			return $batch['jobId'];
		});
	}

	/** @inheritDoc */
	public function setBatchJobId(string $batchKey, string $jobId): void
	{
		$this->transaction(function () use ($batchKey, $jobId) {
			$batch = $this->getValue("batch.{$batchKey}") ?? ['jobId' => null, 'payloads' => []];
			$batch['jobId'] = $jobId;
			$this->setValue("batch.{$batchKey}", $batch);
		});
	}

	/** @inheritDoc */
	public function flushBatchPayloads(string $batchKey): array
	{
		return $this->transaction(function () use ($batchKey) {
			$batch = $this->getValue("batch.{$batchKey}") ?? ['jobId' => null, 'payloads' => []];
			$this->db->prepare('DELETE FROM meta WHERE key = :key')->execute(['key' => "batch.{$batchKey}"]);

			return $batch['payloads'];
		});
	}

	/**
	 * Create the tables if they don't exist yet
	 */
	protected function migrate(): void
	{
		$this->db->exec(
			'CREATE TABLE IF NOT EXISTS jobs (
				id TEXT PRIMARY KEY,
				queue TEXT NOT NULL,
				status TEXT NOT NULL,
				available_at INTEGER NOT NULL,
				created_at INTEGER NOT NULL,
				finished_at INTEGER,
				data TEXT NOT NULL
			)'
		);
		$this->db->exec('CREATE INDEX IF NOT EXISTS jobs_pop ON jobs (queue, status, available_at)');
		$this->db->exec('CREATE INDEX IF NOT EXISTS jobs_status ON jobs (status, created_at)');
		$this->db->exec('CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)');
	}

	/**
	 * Run a callback inside an immediate transaction
	 *
	 * BEGIN IMMEDIATE takes the write lock up front, so two processes can't
	 * read the same row and then both write it.
	 *
	 * @template T
	 * @param callable(): T $callback
	 * @return T
	 */
	protected function transaction(callable $callback): mixed
	{
		// nested call: already inside a transaction
		if ($this->db->inTransaction()) {
			return $callback();
		}

		$this->db->exec('BEGIN IMMEDIATE');

		try {
			$result = $callback();
			$this->db->exec('COMMIT');
			return $result;
		} catch (\Throwable $e) {
			$this->db->exec('ROLLBACK');
			throw $e;
		}
	}

	/**
	 * Read, change and write a job atomically
	 *
	 * @param callable(array<string, mixed>): array<string, mixed> $change
	 */
	protected function modify(string $id, callable $change): void
	{
		$this->transaction(function () use ($id, $change) {
			$job = $this->get($id);

			if ($job !== null) {
				$this->write(array_merge($change($job), ['updated_at' => time()]));
			}
		});
	}

	/**
	 * Insert or replace a job, keeping the indexed columns in sync with its data
	 *
	 * @param array<string, mixed> $job Job data
	 * @return array<string, mixed> The written job data
	 */
	protected function write(array $job): array
	{
		$this->db->prepare(
			'INSERT OR REPLACE INTO jobs (id, queue, status, available_at, created_at, finished_at, data)
			VALUES (:id, :queue, :status, :available_at, :created_at, :finished_at, :data)'
		)->execute([
			'id' => $job['id'],
			'queue' => $job['queue'],
			'status' => $job['status'],
			'available_at' => (int)$job['available_at'],
			'created_at' => (int)$job['created_at'],
			'finished_at' => $job['completed_at'] ?? $job['failed_at'] ?? null,
			'data' => $this->encode($job)
		]);

		return $job;
	}

	/**
	 * Read a value from the meta table
	 */
	protected function getValue(string $key): mixed
	{
		$statement = $this->db->prepare('SELECT value FROM meta WHERE key = :key');
		$statement->execute(['key' => $key]);

		$value = $statement->fetchColumn();

		return is_string($value) ? $this->decode($value) : null;
	}

	/**
	 * Write a value to the meta table
	 */
	protected function setValue(string $key, mixed $value): void
	{
		$this->db->prepare('INSERT OR REPLACE INTO meta (key, value) VALUES (:key, :value)')->execute([
			'key' => $key,
			'value' => $this->encode($value)
		]);
	}

	protected function encode(mixed $value): string
	{
		return json_encode($value, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRESERVE_ZERO_FRACTION | JSON_INVALID_UTF8_SUBSTITUTE);
	}

	protected function decode(string $value): mixed
	{
		return json_decode($value, true, flags: JSON_THROW_ON_ERROR);
	}
}
