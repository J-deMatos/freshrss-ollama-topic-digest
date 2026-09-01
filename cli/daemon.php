#!/usr/bin/env php
<?php
declare(strict_types=1);

define('TOPIC_DIGEST_WORKER', true);
require_once __DIR__ . '/bootstrap.php';
topicDigestLoadFreshRssCli();

$options = getopt('', ['user:', 'batch:', 'max-runtime:', 'concurrency:']);
if (!is_array($options)) {
	fail('Cannot parse Topic Digest daemon options.');
}
$username = is_string($options['user'] ?? null) ? $options['user'] : '';
$batch = is_numeric($options['batch'] ?? null) ? (int)$options['batch'] : 20;
$maxRuntime = is_numeric($options['max-runtime'] ?? null) ? (int)$options['max-runtime'] : 0;
$concurrencyOption = is_numeric($options['concurrency'] ?? null) ? (int)$options['concurrency'] : null;
if ($username === '' || $batch < 1 || $batch > 100 || $maxRuntime < 0
		|| ($concurrencyOption !== null && ($concurrencyOption < 1 || $concurrencyOption > 8))) {
	fail('Usage: daemon.php --user USER [--batch 20] [--max-runtime 0] [--concurrency 1]');
}
cliInitUser($username);
$extension = Minz_ExtensionManager::findExtension('Topic Digest');
if (!($extension instanceof TopicDigestExtension) || !$extension->isEnabled()) {
	fail('Topic Digest is not enabled for this user.');
}
// --concurrency overrides the configured worker_concurrency for this invocation only; the setting itself is
// what launchAutomaticWorker() passes down, so this only matters for a manually-run daemon.
$concurrency = $concurrencyOption ?? $extension->workerConcurrency();
$lock = fopen($extension->lockPath(), 'c');
if ($lock === false) {
	fail('Cannot open the Topic Digest worker lock.');
}
if (!flock($lock, LOCK_EX | LOCK_NB)) {
	exit(0);
}
$startedAt = time();
$reload = false;
$fingerprint = static function () use ($username): string {
	$paths = array_merge(
		glob(dirname(__DIR__) . '/*.php') ?: [],
		[__FILE__, USERS_PATH . '/' . $username . '/config.php']
	);
	sort($paths);
	$context = hash_init('sha256');
	foreach ($paths as $path) {
		hash_update($context, $path . "\0");
		if (is_file($path)) {
			hash_update_file($context, $path);
		}
	}
	return hash_final($context);
};
$startingFingerprint = $fingerprint();
$startingRestartRevision = $extension->store()->workerRestartRevision();
try {
	do {
		$result = ['processed' => 0, 'failed' => 0, 'backfill_scanned' => 0];
		// One processor per batch, not per article: constructing it re-reads the whole configuration and the
		// profile bookkeeping. Any configuration change alters the fingerprint below and reloads the daemon
		// outright, so a processor never outlives the settings it was built from.
		$processor = new TopicDigestProcessor($extension, $concurrency);
		// --batch means "up to N articles total this invocation," unchanged by concurrency: each run() call
		// below claims one wavefront of at most $concurrency articles, and the loop steps by jobs actually
		// claimed (not merely processed/failed — a stale job silently requeued still consumes one attempt,
		// exactly as one for-loop iteration did before wavefronts existed) so a run of persistently-requeued
		// jobs cannot loop past what --batch bounds. The hot-reload fingerprint check below then runs once per
		// wavefront instead of once per article when concurrency > 1 — a deliberate, bounded coarsening (at
		// most $concurrency articles' worth of delay), not a behavior change to what --batch bounds.
		$claimedInBatch = 0;
		while ($claimedInBatch < $batch) {
			$single = $processor->run(min($concurrency, $batch - $claimedInBatch));
			$result['processed'] += $single['processed'];
			$result['failed'] += $single['failed'];
			$result['backfill_scanned'] += $single['backfill_scanned'];
			$claimedInBatch += $single['claimed'];
			if (!hash_equals($startingFingerprint, $fingerprint())) {
				$reload = true;
				break;
			}
			if ($extension->store()->workerRestartRevision() !== $startingRestartRevision) {
				$reload = true;
				break;
			}
			if ($single['processed'] === 0 && $single['failed'] === 0) {
				break;
			}
		}
		$status = $extension->store()->status();
		// "errors", not "failed": this counts failures in this batch including ones that will be retried, whereas
		// the status page's "Failed" counts only jobs that have exhausted their retries. Reporting both under the
		// same name made the two look like they contradicted each other.
		echo date(DATE_ATOM), ': processed ', $result['processed'], '; errors ', $result['failed'],
			'; awaiting retry ', $status['retrying'], '; failed permanently ', $status['failed'],
			'; scanned ', $result['backfill_scanned'], '; queued ', $status['queued'], ".\n";
		if ($reload || (int)$status['paused'] !== 0
				|| ((int)$status['queued'] === 0 && (int)$status['backfill_active'] === 0)) {
			break;
		}
		if ($result['processed'] === 0 && $result['failed'] === 0) {
			sleep(5);
		}
		gc_collect_cycles();
	} while ($maxRuntime === 0 || time() - $startedAt < $maxRuntime);
} catch (Throwable $e) {
	Minz_Log::error('Topic Digest daemon error: ' . $e->getMessage());
	fwrite(STDERR, date(DATE_ATOM) . ': ' . $e->getMessage() . "\n");
} finally {
	flock($lock, LOCK_UN);
	fclose($lock);
}

if ($reload && function_exists('exec')) {
	$remainingRuntime = $maxRuntime === 0 ? 0 : max(1, $maxRuntime - (time() - $startedAt));
	$logPath = dirname($extension->lockPath()) . '/worker.log';
	$command = 'nohup ' . escapeshellarg(PHP_BINARY)
		. ' ' . escapeshellarg(__FILE__)
		. ' --user ' . escapeshellarg($username)
		. ' --batch ' . escapeshellarg((string)$batch)
		. ' --max-runtime ' . escapeshellarg((string)$remainingRuntime)
		// Re-reads the freshly-changed configuration on restart rather than re-passing the option this
		// process happened to start with, unless the option was explicitly given on the command line.
		. ($concurrencyOption !== null ? ' --concurrency ' . escapeshellarg((string)$concurrencyOption) : '')
		. ' >> ' . escapeshellarg($logPath) . ' 2>&1 < /dev/null &';
	$output = [];
	$resultCode = 0;
	exec($command, $output, $resultCode);
	if ($resultCode !== 0) {
		Minz_Log::error("Topic Digest daemon reload failed with status {$resultCode}.");
	}
} elseif ($reload) {
	Minz_Log::error('Topic Digest daemon cannot reload itself because PHP exec() is disabled.');
}
