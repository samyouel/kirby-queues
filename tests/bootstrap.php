<?php

use Kirby\Cms\App;

const KIRBY_HELPER_DUMP = false;
const KIRBY_HELPER_E = false;

require_once __DIR__ . '/../vendor/autoload.php';

require_once __DIR__ . '/Jobs/TestJob.php';
require_once __DIR__ . '/Jobs/FailingTestJob.php';
require_once __DIR__ . '/Jobs/ProcessDataJob.php';
require_once __DIR__ . '/Jobs/ScheduledTestJob.php';
require_once __DIR__ . '/Jobs/BatchTestJob.php';

require_once __DIR__ . '/../index.php';

$app = new App([
	'roots' => [
		'index' => __DIR__,
		'base' => __DIR__,
	],
	'options' => [
		// run the suite against another storage with QUEUES_STORAGE=sqlite
		'tobimori.queues.storage' => getenv('QUEUES_STORAGE') ?: 'cache',
		'tobimori.queues.sqlite.path' => __DIR__ . '/site/cache/queues-test.sqlite',
	],
]);

// Register test job types
\tobimori\Queues\Queues::register(
	\tobimori\Queues\Tests\Jobs\TestJob::class,
	\tobimori\Queues\Tests\Jobs\FailingTestJob::class,
	\tobimori\Queues\Tests\Jobs\ProcessDataJob::class,
	\tobimori\Queues\Tests\Jobs\ScheduledTestJob::class,
	\tobimori\Queues\Tests\Jobs\BatchTestJob::class
);
