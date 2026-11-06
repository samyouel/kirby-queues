<?php

/**
 * Pushes jobs into a SQLite queue from a separate process.
 *
 * Used by SqliteStorageTest to run several writers at the same time.
 * Usage: php concurrent-push.php <database path> <prefix> <count>
 */

use tobimori\Queues\Storage\SqliteStorage;

require_once __DIR__ . '/../../vendor/autoload.php';

[, $path, $prefix, $count] = $argv;

$storage = new SqliteStorage($path);

for ($i = 0; $i < (int)$count; $i++) {
	$storage->push('default', [
		'id' => "{$prefix}-{$i}",
		'type' => 'test',
		'payload' => ['i' => $i],
		'attempts' => 0
	]);
}
