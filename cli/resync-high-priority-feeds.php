#!/usr/bin/env php
<?php
declare(strict_types=1);

/**
 * Migrates existing high-priority ("feed" presentation) topics to the one-entry-per-event standard.
 *
 * Before this change, every matched source article got its own entry in a high-priority feed, so an event
 * covered by several sources (near-duplicate republishes, the same story from more than one feed) showed up as
 * that many separate entries. Upgrading the extension code already makes new matches follow the new rule and
 * backfills which source is each existing event's primary, but the extra entries an already-running instance
 * created under the old rule are only actually removed the next time that topic's high-priority feed is
 * resynchronised with pruning enabled. This is the lightweight way to do that for every affected topic at once,
 * without going through "Restart and rebuild", which also wipes and re-derives every classification decision
 * (an Ollama call per previously matched article) to get there.
 *
 * Reads matches, events and summaries as they already are; makes no Ollama calls and requeues no jobs.
 */

define('TOPIC_DIGEST_WORKER', true);
require_once __DIR__ . '/bootstrap.php';
topicDigestLoadFreshRssCli();

$options = getopt('', ['user:', 'apply']);
if (!is_array($options)) {
	fail('Cannot parse Topic Digest resync options.');
}
$username = is_string($options['user'] ?? null) ? $options['user'] : '';
$apply = array_key_exists('apply', $options);
if ($username === '') {
	fail('Usage: resync-high-priority-feeds.php --user USER [--apply]');
}

cliInitUser($username);
$extension = Minz_ExtensionManager::findExtension('Topic Digest');
if (!($extension instanceof TopicDigestExtension) || !$extension->isEnabled()) {
	fail('Topic Digest is not enabled for this user.');
}

// synchroniseTopic() writes to the same synthetic feeds and entries the worker maintains, so refuse to run
// beside it rather than racing it.
$lock = fopen($extension->lockPath(), 'c');
if ($lock === false || !flock($lock, LOCK_EX | LOCK_NB)) {
	fail('The Topic Digest worker is running. Stop it, or pause processing, and try again.');
}

/** Number of entries currently materialised in a topic's high-priority feed. */
function countFeedEntries(int $feedId): int {
	$count = 0;
	foreach (FreshRSS_Factory::createEntryDao()->listWhere(type: 'f', id: $feedId, state: FreshRSS_Entry::STATE_ALL, limit: -1) as $ignored) {
		$count++;
	}
	return $count;
}

try {
	$store = $extension->store();
	$topics = array_values(array_filter($store->topics(), static fn(array $topic): bool => $topic['topic_type'] === 'feed'));
	if ($topics === []) {
		echo "No high-priority topics to resync.\n";
		exit(0);
	}
	$totalBefore = 0;
	$totalTargets = 0;
	foreach ($topics as $topic) {
		$feedId = (int)($topic['feed_id'] ?? 0);
		$before = $feedId > 0 ? countFeedEntries($feedId) : 0;
		$target = count($store->events((int)$topic['id']));
		$totalBefore += $before;
		$totalTargets += $target;
		printf("Topic \"%s\": %d entries now, %d events (%s).\n", $topic['name'], $before, $target,
			$before > $target ? 'will drop to ' . $target : 'already at or below target');
		if ($apply) {
			$extension->synchroniseTopic((int)$topic['id'], false, true);
			$after = $feedId > 0 ? countFeedEntries($feedId) : count($store->events((int)$topic['id']));
			printf("  -> resynced, %d entries now.\n", $after);
		}
	}
	if (!$apply) {
		printf("Dry run: %d entries across %d topics, %d events in total. Pass --apply to resync them.\n",
			$totalBefore, count($topics), $totalTargets);
	}
} catch (Throwable $e) {
	fail('Topic Digest resync failed: ' . $e->getMessage());
} finally {
	flock($lock, LOCK_UN);
	fclose($lock);
}
