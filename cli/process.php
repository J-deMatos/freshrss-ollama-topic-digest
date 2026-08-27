#!/usr/bin/env php
<?php
declare(strict_types=1);

define('TOPIC_DIGEST_WORKER', true);
require dirname(__DIR__, 3) . '/cli/_cli.php';

$options = getopt('', ['user:', 'limit:']);
$username = is_string($options['user'] ?? null) ? $options['user'] : '';
$limit = is_numeric($options['limit'] ?? null) ? (int)$options['limit'] : 20;
if ($username === '' || $limit < 1 || $limit > 1000) {
	fail('Usage: process.php --user USER [--limit 20]');
}
cliInitUser($username);
$extension = Minz_ExtensionManager::findExtension('Topic Digest');
if (!($extension instanceof TopicDigestExtension) || !$extension->isEnabled()) {
	fail('Topic Digest is not enabled for this user.');
}
$lock = fopen($extension->lockPath(), 'c');
if ($lock === false || !flock($lock, LOCK_EX | LOCK_NB)) {
	fail('Topic Digest worker is already running or cannot acquire its lock.');
}
try {
	$result = (new TopicDigestProcessor($extension))->run($limit);
	echo 'Topic Digest processed ', $result['processed'], ' jobs; ', $result['failed'],
		' failed; scanned ', $result['backfill_scanned'], " archive entries.\n";
} catch (Throwable $e) {
	fail('Topic Digest failed: ' . $e->getMessage());
} finally {
	flock($lock, LOCK_UN);
	fclose($lock);
}
