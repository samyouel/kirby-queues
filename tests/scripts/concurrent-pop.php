<?php

/**
 * Pops jobs from a SQLite queue until it is empty and prints their IDs.
 *
 * Used by SqliteStorageTest to run several workers at the same time.
 * Usage: php concurrent-pop.php <database path>
 */

use tobimori\Queues\Storage\SqliteStorage;

require_once __DIR__ . '/../../vendor/autoload.php';

$storage = new SqliteStorage($argv[1]);

while ($job = $storage->pop('default')) {
	echo $job['id'] . PHP_EOL;
}
