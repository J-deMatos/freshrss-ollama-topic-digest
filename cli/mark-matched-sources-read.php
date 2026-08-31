#!/usr/bin/env php
<?php
declare(strict_types=1);

/**
 * One-off cleanup for matched source articles that were never marked read in their original feed.
 *
 * TopicDigestProcessor::process() already marks a matched source read in its home feed as soon as the match is
 * persisted, but only when the entry is still unread, unchanged since the match, and not a favourite or
 * otherwise deliberately kept unread. During earlier development that step went through changes and gaps of its
 * own, so an installation that has been running across those changes can accumulate matches whose source never
 * actually got marked read, even though it is faithfully represented in the digest or high-priority feed. This
 * retroactively applies the same read-marking rule to every currently matched source, once.
 *
 * Never touches anything the extension already treats as off-limits: favourites, entries the user manually kept
 * unread, entries a rebuild is still deciding whether to restore, and entries whose content has changed since
 * they matched (those are either already re-queued for reclassification or waiting to be, so marking them read
 * off a stale match could be wrong).
 */

define('TOPIC_DIGEST_WORKER', true);
require_once __DIR__ . '/bootstrap.php';
topicDigestLoadFreshRssCli();

$options = getopt('', ['user:', 'apply']);
if (!is_array($options)) {
	fail('Cannot parse Topic Digest mark-read options.');
}
$username = is_string($options['user'] ?? null) ? $options['user'] : '';
$apply = array_key_exists('apply', $options);
if ($username === '') {
	fail('Usage: mark-matched-sources-read.php --user USER [--apply]');
}

cliInitUser($username);
$extension = Minz_ExtensionManager::findExtension('Topic Digest');
if (!($extension instanceof TopicDigestExtension) || !$extension->isEnabled()) {
	fail('Topic Digest is not enabled for this user.');
}

// markRead() below is the same write the worker performs on a fresh match; refuse to race it.
$lock = fopen($extension->lockPath(), 'c');
if ($lock === false || !flock($lock, LOCK_EX | LOCK_NB)) {
	fail('The Topic Digest worker is running. Stop it, or pause processing, and try again.');
}

try {
	$store = $extension->store();
	$entryDao = FreshRSS_Factory::createEntryDao();
	$eligible = 0;
	$marked = 0;
	$skipped = ['already read' => 0, 'gone' => 0, 'favourite' => 0, 'manually kept unread' => 0,
		'kept unread by a rebuild' => 0, 'changed since it matched' => 0];
	foreach ($store->matchedSourceEntryIds() as $entryId) {
		$entry = $entryDao->searchById($entryId);
		if ($entry === null) {
			$skipped['gone']++;
			continue;
		}
		if ($entry->isRead()) {
			$skipped['already read']++;
			continue;
		}
		if ($entry->isFavorite()) {
			$skipped['favourite']++;
			continue;
		}
		if ($store->isProtected($entryId)) {
			$skipped['manually kept unread']++;
			continue;
		}
		if ($store->isRebuildUnread($entryId)) {
			$skipped['kept unread by a rebuild']++;
			continue;
		}
		if (!$store->hasMatchingSourceContentHash($entryId, $entry->hash())) {
			$skipped['changed since it matched']++;
			continue;
		}
		$eligible++;
		if ($apply) {
			if ($entryDao->markRead($entryId, true) === false) {
				throw new RuntimeException("Could not mark entry {$entryId} read.");
			}
			$marked++;
		}
	}
	foreach ($skipped as $reason => $count) {
		if ($count > 0) {
			echo "Left alone ({$reason}): {$count}.\n";
		}
	}
	if ($apply) {
		echo "Marked read: {$marked}.\n";
	} else {
		echo "Would mark read: {$eligible}. Pass --apply to do it.\n";
	}
} catch (Throwable $e) {
	fail('Topic Digest mark-read cleanup failed: ' . $e->getMessage());
} finally {
	flock($lock, LOCK_UN);
	fclose($lock);
}
