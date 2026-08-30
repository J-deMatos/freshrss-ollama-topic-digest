#!/usr/bin/env php
<?php
declare(strict_types=1);

define('TOPIC_DIGEST_WORKER', true);
define('TOPIC_DIGEST_CHILD_WORKER', true);
require_once __DIR__ . '/bootstrap.php';
topicDigestLoadFreshRssCli();

$options = getopt('', ['user:', 'coordinator-token:']);
$username = is_string($options['user'] ?? null) ? $options['user'] : '';
$coordinatorToken = is_string($options['coordinator-token'] ?? null) ? $options['coordinator-token'] : '';
if ($username === '' || preg_match('/^[a-f0-9]{64}$/D', $coordinatorToken) !== 1) {
	fail('Topic Digest child workers may only be launched by the coordinator.');
}
cliInitUser($username);
$extension = Minz_ExtensionManager::findExtension('Topic Digest');
if (!($extension instanceof TopicDigestExtension) || !$extension->isEnabled()) {
	fail('Topic Digest is not enabled for this user.');
}
try {
	echo json_encode((new TopicDigestProcessor($extension))->runWorker(), JSON_THROW_ON_ERROR), "\n";
} catch (Throwable $e) {
	Minz_Log::error('Topic Digest child worker error: ' . $e->getMessage());
	echo json_encode(['processed' => 0, 'failed' => 1, 'backfill_scanned' => 0,
		'throttle_status' => 0, 'retry_after' => -1], JSON_THROW_ON_ERROR), "\n";
}
