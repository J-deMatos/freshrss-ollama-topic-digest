<?php
declare(strict_types=1);

$freshRssPath = getenv('FRESHRSS_PATH');
if (!is_string($freshRssPath) || $freshRssPath === '') {
	$freshRssPath = dirname(__DIR__, 3);
}
$freshRssPath = realpath($freshRssPath) ?: '';
if ($freshRssPath === '' || !is_file($freshRssPath . '/tests/bootstrap.php')) {
	throw new RuntimeException(
		'Set FRESHRSS_PATH to a FreshRSS source checkout before running the extension tests.'
	);
}

require $freshRssPath . '/tests/bootstrap.php';
