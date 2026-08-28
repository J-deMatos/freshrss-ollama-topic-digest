<?php
declare(strict_types=1);

function topicDigestLoadFreshRssCli(): void {
	$environmentPath = getenv('FRESHRSS_PATH');
	$candidates = [
		is_string($environmentPath) ? $environmentPath : '',
		dirname(__DIR__, 3),
		'/app/www',
		'/var/www/FreshRSS',
		'/var/www/freshrss',
		'/usr/share/FreshRSS',
	];
	$checked = [];
	foreach ($candidates as $root) {
		$root = rtrim($root, '/');
		if ($root === '' || isset($checked[$root])) {
			continue;
		}
		$checked[$root] = true;
		$bootstrap = $root . '/cli/_cli.php';
		if (is_file($bootstrap)) {
			require_once $bootstrap;
			return;
		}
	}
	throw new RuntimeException('Cannot locate FreshRSS cli/_cli.php. Set FRESHRSS_PATH to the FreshRSS application directory.');
}
