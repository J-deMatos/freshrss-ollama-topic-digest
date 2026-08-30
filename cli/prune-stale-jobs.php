#!/usr/bin/env php
<?php
declare(strict_types=1);

/**
 * Clears queue rows that are bookkeeping residue rather than articles.
 *
 * They were queued before the GUID column existed and are keyed by an id FreshRSS renumbered when it committed the
 * entry, so nothing can resolve them back to an article. Left alone, the worker claims each one, looks the entry up,
 * finds nothing and records a permanent "skipped" row, which inflates "Articles processed" and buries the real skip
 * reasons. Deleting them loses no work: an article that still exists is queued again under its current id by the
 * archive scan.
 *
 * Run the archive scan to completion first. Until it has, a row queued before the column existed whose article is
 * still live is indistinguishable from residue, and this would delete it — the article would come back on the next
 * scan, but the queue position and any retry history would not.
 */

define('TOPIC_DIGEST_WORKER', true);
require_once __DIR__ . '/bootstrap.php';
topicDigestLoadFreshRssCli();

$options = getopt('', ['user:', 'apply']);
if (!is_array($options)) {
	fail('Cannot parse Topic Digest prune options.');
}
$username = is_string($options['user'] ?? null) ? $options['user'] : '';
$apply = array_key_exists('apply', $options);
if ($username === '') {
	fail('Usage: prune-stale-jobs.php --user USER [--apply]');
}

cliInitUser($username);
$extension = Minz_ExtensionManager::findExtension('Topic Digest');
if (!($extension instanceof TopicDigestExtension) || !$extension->isEnabled()) {
	fail('Topic Digest is not enabled for this user.');
}

// The worker rewrites the same rows, so refuse to run beside it rather than racing it.
$lock = fopen($extension->lockPath(), 'c');
if ($lock === false || !flock($lock, LOCK_EX | LOCK_NB)) {
	fail('The Topic Digest worker is running. Stop it, or pause processing, and try again.');
}

try {
	$store = $extension->store();
	// Opening the store already drops residue rows that were recorded as skipped, retroactively — they hold no
	// work, so that needs no confirmation. Report it rather than letting the counters change silently.
	$pending = $store->stalePendingJobCount();
	$backfillActive = $store->backfill()['active'];
	echo 'Already recorded as skipped, dropped just now: ', $store->discardedSkipsOnOpen(), ".\n";
	echo 'Stale queue rows still waiting to be processed: ', $pending, ".\n";
	echo 'Discarded in total so far: ', $store->staleJobsDiscarded(), ".\n";
	if ($backfillActive) {
		echo "The archive scan has not finished. Let it complete before pruning: until it has, a row queued\n",
			"before GUIDs were recorded cannot be told apart from one whose article is still live.\n";
	}
	if ($pending === 0) {
		echo "Nothing left to prune.\n";
	} elseif (!$apply) {
		echo "Dry run: pass --apply to delete them.\n";
	} elseif ($backfillActive) {
		fail('Refusing to prune while the archive scan is active.');
	} else {
		echo 'Deleted ', $store->pruneStalePendingJobs(), " stale queue rows.\n";
	}
} catch (Throwable $e) {
	fail('Topic Digest prune failed: ' . $e->getMessage());
} finally {
	flock($lock, LOCK_UN);
	fclose($lock);
}
