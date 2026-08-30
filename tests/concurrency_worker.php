#!/usr/bin/env php
<?php
declare(strict_types=1);

require_once dirname(__DIR__) . '/TopicDigestStore.php';

$mode = $argv[1] ?? '';
if ($mode === 'claim') {
	$store = new TopicDigestStore((string)$argv[2]);
	$job = $store->claim(600);
	if ($job !== null) {
		usleep((int)($argv[3] ?? 0));
		echo (string)$job['entry_id'], "\n";
	}
	exit(0);
}
if ($mode === 'same-event') {
	$database = (string)$argv[2];
	$lockPath = (string)$argv[3];
	$topicId = (int)$argv[4];
	$entryId = (string)$argv[5];
	$lock = fopen($lockPath, 'c');
	if ($lock === false || !flock($lock, LOCK_EX)) {
		exit(2);
	}
	try {
		$store = new TopicDigestStore($database);
		$events = $store->events($topicId);
		usleep(100000);
		$store->addMatch($topicId, [
			'entry_id' => $entryId, 'title' => 'Model X report', 'link' => 'https://example.com/' . $entryId,
			'published_at' => 1_700_000_000, 'content_hash' => hash('sha256', $entryId), 'pipeline_hash' => 'pipeline',
		], 'Feed', 'Model X released', 1_700_000_000, 'Same release', [1.0],
			$events === [] ? null : (int)$events[0]['id']);
	} finally {
		flock($lock, LOCK_UN);
		fclose($lock);
	}
	exit(0);
}
if ($mode === 'delay') {
	usleep((int)($argv[2] ?? 0));
	exit(0);
}
if ($mode === 'locked-delay') {
	$lock = fopen((string)$argv[2], 'c');
	if ($lock === false || !flock($lock, LOCK_EX)) {
		exit(2);
	}
	try {
		usleep((int)($argv[3] ?? 0));
	} finally {
		flock($lock, LOCK_UN);
		fclose($lock);
	}
	exit(0);
}
exit(2);
