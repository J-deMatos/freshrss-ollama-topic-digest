#!/usr/bin/env php
<?php
declare(strict_types=1);

$jobs = 100;
$delayMicroseconds = 50000;
$worker = __DIR__ . '/concurrency_worker.php';
foreach ([1, 2, 4, 8] as $concurrency) {
	$started = hrtime(true);
	$launched = 0;
	$active = [];
	while ($launched < $jobs || $active !== []) {
		while ($launched < $jobs && count($active) < $concurrency) {
			$process = proc_open([PHP_BINARY, $worker, 'delay', (string)$delayMicroseconds],
				[0 => ['file', '/dev/null', 'r'], 1 => ['file', '/dev/null', 'w'], 2 => ['file', '/dev/null', 'w']], $pipes);
			if (!is_resource($process)) {
				throw new RuntimeException('Could not launch the fake cloud request.');
			}
			$active[] = $process;
			$launched++;
		}
		foreach ($active as $key => $process) {
			$status = proc_get_status($process);
			if ($status === false) {
				throw new RuntimeException('Could not inspect the fake cloud request.');
			}
			if (!$status['running']) {
				proc_close($process);
				unset($active[$key]);
			}
		}
		if ($active !== []) {
			usleep(1000);
		}
	}
	$seconds = (hrtime(true) - $started) / 1_000_000_000;
	echo "100 fake articles, concurrency {$concurrency}: ", number_format($seconds, 3), " seconds\n";
}
