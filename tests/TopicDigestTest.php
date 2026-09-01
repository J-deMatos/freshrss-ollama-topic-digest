<?php
declare(strict_types=1);

use PHPUnit\Framework\TestCase;

require_once dirname(__DIR__) . '/TopicDigestStore.php';
require_once dirname(__DIR__) . '/ArticleSummaryCache.php';
require_once dirname(__DIR__) . '/TopicDigestTextProvider.php';
require_once dirname(__DIR__) . '/TopicDigestOllama.php';
require_once dirname(__DIR__) . '/TopicDigestOpenAICompatible.php';
require_once dirname(__DIR__) . '/TopicDigestTextProviderChain.php';
require_once dirname(__DIR__) . '/TopicDigestConcurrentDispatcher.php';
require_once dirname(__DIR__) . '/TopicDigestProcessor.php';

final class TopicDigestTest extends TestCase {
	private string $databasePath;
	private TopicDigestStore $store;

	#[\Override]
	protected function setUp(): void {
		$this->databasePath = sys_get_temp_dir() . '/freshrss-topic-digest-' . bin2hex(random_bytes(8)) . '.sqlite';
		$this->store = new TopicDigestStore($this->databasePath);
	}

	#[\Override]
	protected function tearDown(): void {
		foreach ([$this->databasePath, $this->databasePath . '-wal', $this->databasePath . '-shm'] as $path) {
			if (file_exists($path)) {
				unlink($path);
			}
		}
	}

	public function testTopicDefaultsAndRuleChangesInvalidateEmbedding(): void {
		$id = $this->store->saveTopic([
			'name' => 'AI releases', 'description' => 'New publicly released AI models.',
			'enabled' => true, 'all_feeds' => true,
		]);
		$topic = $this->store->topic($id);
		self::assertSame('days', $topic['backfill_mode']);
		self::assertSame(90, $topic['backfill_days']);
		self::assertSame('digest', $topic['topic_type']);
		self::assertTrue($this->store->saveTopicEmbedding($id, $topic['rule_hash'], [1.0, 0.0]));

		$topic['description'] = 'Only announcements of newly available foundation models.';
		$this->store->saveTopic($topic, $id);
		self::assertNull($this->store->topic($id)['description_embedding']);
	}

	public function testTopicCanUseHighPriorityFeedPresentation(): void {
		$id = $this->store->saveTopic([
			'name' => 'Security incidents', 'description' => 'Confirmed security incidents.',
			'enabled' => true, 'all_feeds' => true, 'topic_type' => 'feed',
		]);
		self::assertSame('feed', $this->store->topic($id)['topic_type']);

		$topic = $this->store->topic($id);
		$topic['topic_type'] = 'digest';
		$this->store->saveTopic($topic, $id);
		self::assertSame('digest', $this->store->topic($id)['topic_type']);
	}

	public function testTopicCanMarkMatchesReadWithoutAFeedOrExposeVerification(): void {
		$id = $this->store->saveTopic([
			'name' => 'Routine updates', 'description' => 'Routine product updates.',
			'enabled' => true, 'all_feeds' => true, 'topic_type' => 'mark_read',
		]);
		$topic = $this->store->topic($id);
		self::assertSame('mark_read', $topic['topic_type']);
		self::assertFalse($topic['show_verification']);

		$topic['show_verification'] = true;
		$this->store->saveTopic($topic, $id);
		self::assertTrue($this->store->topic($id)['show_verification']);
		$topic = $this->store->topic($id);
		$topic['topic_type'] = 'digest';
		$this->store->saveTopic($topic, $id);
		self::assertFalse($this->store->topic($id)['show_verification']);
	}

	public function testQueueIsIdempotentAndPipelineChangesRequeue(): void {
		$entry = new FreshRSS_Entry(4, 'guid', 'Model released', '', 'Detailed release announcement.',
			'https://example.com/model', 1_700_000_000);
		$entry->_id('1700000000000000');
		self::assertTrue($this->store->enqueue($entry, 2, 'pipeline-one'));
		self::assertFalse($this->store->enqueue($entry, 2, 'pipeline-one'));
		self::assertTrue($this->store->enqueue($entry, 2, 'pipeline-two'));
	}

	public function testPendingClassificationOnlyBlocksTheMatchingArticleRevision(): void {
		$entry = new FreshRSS_Entry(4, 'guid', 'Model released', '', 'Detailed release announcement.',
			'https://example.com/model', 1_700_000_000);
		$entry->_id('1700000000000000');
		$this->store->enqueue($entry, 2, 'pipeline');
		self::assertTrue($this->store->classificationPending($entry->id(), $entry->hash()));
		self::assertFalse($this->store->classificationPending($entry->id(), 'different-content'));

		$job = $this->store->claim(600);
		self::assertNotNull($job);
		self::assertTrue($this->store->classificationPending($entry->id(), $entry->hash()));
		self::assertTrue($this->store->completeCurrent($job, 'skipped'));
		self::assertFalse($this->store->classificationPending($entry->id(), $entry->hash()));
	}

	public function testMultipleSourcesJoinOneEventAndSourceRestoreIsPersistent(): void {
		$topicId = $this->store->saveTopic(['name' => 'AI', 'description' => 'AI model releases',
			'enabled' => true, 'all_feeds' => true]);
		$first = $this->job('100', 'First report');
		$result = $this->store->addMatch($topicId, $first, 'Feed A', 'Model X released', 1_700_000_000,
			'It announces availability.', [1.0, 0.0], null);
		$second = $this->job('200', 'Second report');
		$this->store->addMatch($topicId, $second, 'Feed B', 'Model X released', 1_700_000_100,
			'It covers the same release.', [0.99, 0.01], $result['event_id']);
		self::assertCount(2, $this->store->events($topicId)[0]['sources']);
		self::assertSame(2, $this->store->topic($topicId)['article_count']);
		self::assertSame(1, $this->store->topic($topicId)['event_count']);
		self::assertSame('It announces availability.', $this->store->source($topicId, '100')['explanation']);

		self::assertSame(['100'], $this->store->restoreSource($topicId, '100'));
		self::assertTrue($this->store->isRejected($topicId, '100'));
		self::assertCount(1, $this->store->events($topicId)[0]['sources']);
		self::assertSame(1, $this->store->topic($topicId)['article_count']);
		self::assertCount(1, $this->store->suggestions($topicId));
	}

	public function testEventsRemainOrderedByEffectiveDateWhenOlderEventsReceiveCoverage(): void {
		$topicId = $this->store->saveTopic(['name' => 'AI', 'description' => 'AI model releases',
			'enabled' => true, 'all_feeds' => true]);
		$first = $this->store->addMatch($topicId, $this->job('100', 'First event'), 'Feed', 'First event',
			1_700_000_000, 'First event', [1.0], null);
		$second = $this->store->addMatch($topicId, $this->job('200', 'Second event'), 'Feed', 'Second event',
			1_700_000_100, 'Second event', [1.0], null);
		$coverage = $this->job('300', 'New coverage of first');
		$coverage['published_at'] = 1_700_000_200;
		$this->store->addMatch($topicId, $coverage, 'Feed', 'First event',
			1_700_000_200, 'New coverage', [1.0], $first['event_id']);

		$events = $this->store->events($topicId);
		self::assertSame($second['event_id'], (int)$events[0]['id']);
		self::assertSame($first['event_id'], (int)$events[1]['id']);
		self::assertSame('300', $events[1]['sources'][0]['entry_id']);
	}

	public function testClassifierRevisionStartsOnlyOneBackfill(): void {
		$this->store->saveTopic(['name' => 'AI', 'description' => 'AI model releases',
			'enabled' => true, 'all_feeds' => true]);
		self::assertTrue($this->store->ensureClassifierRevision('topic-context-v2'));
		self::assertTrue($this->store->backfill()['active']);
		$revision = $this->store->pipelineRevision();
		self::assertFalse($this->store->ensureClassifierRevision('topic-context-v2'));
		self::assertSame($revision, $this->store->pipelineRevision());
	}

	public function testWorkerRestartRevisionIsPersistentAndMonotonic(): void {
		self::assertSame(0, $this->store->workerRestartRevision());
		self::assertSame(1, $this->store->requestWorkerRestart());
		self::assertSame(1, $this->store->workerRestartRevision());
		self::assertSame(2, $this->store->requestWorkerRestart());
		self::assertSame(2, $this->store->workerRestartRevision());
	}

	public function testDigestRebuildClearsMatchesAndStagesFormerSourcesForRestore(): void {
		$topicId = $this->store->saveTopic(['name' => 'AI', 'description' => 'AI model releases',
			'enabled' => true, 'all_feeds' => true]);
		$this->store->addMatch($topicId, $this->job('100', 'Wrong match'), 'Feed', 'Wrong event',
			1_700_000_000, 'Old decision', [1.0], null);
		$revision = $this->store->pipelineRevision();

		self::assertSame(['100'], $this->store->rebuildDigests());
		self::assertSame([], $this->store->events($topicId));
		self::assertTrue($this->store->isPendingRebuildRestore('100'));
		self::assertSame([$topicId], $this->store->previousTopicIdsForRebuild('100'));
		self::assertSame($revision + 1, $this->store->pipelineRevision());
		self::assertTrue($this->store->backfill()['active']);

		$this->store->recordRebuildUnread('100');
		self::assertTrue($this->store->isRebuildUnread('100'));
		self::assertFalse($this->store->isProtected('100'));
		$this->store->completeRebuildRestore('100');
		self::assertFalse($this->store->isPendingRebuildRestore('100'));
		self::assertSame([], $this->store->previousTopicIdsForRebuild('100'));
	}

	public function testRebuildStampsThePipelineHashEvenOnJobsCompletedDuringTheRebuild(): void {
		$entry = new FreshRSS_Entry(4, 'guid', 'Model released', '', 'Detailed release announcement.',
			'https://example.com/model', 1_700_000_000);
		$entry->_id('1700000000000000');
		$this->store->enqueue($entry, 2, 'pipeline-one');
		$this->store->rebuildDigests();
		// The worker can finish a job between rebuildDigests() and prepareRebuildJobs(); it must still be
		// restamped, or it keeps an empty pipeline hash forever and the restart silently never reprocesses it.
		$job = $this->store->claim(600);
		self::assertNotNull($job);
		self::assertTrue($this->store->completeCurrent($job, 'done'));

		self::assertSame(1, $this->store->prepareRebuildJobs('pipeline-two'));
		self::assertTrue($this->store->classificationPending($entry->id(), $entry->hash()));
		self::assertFalse($this->store->enqueue($entry, 2, 'pipeline-two'));
	}

	public function testRebuildPrioritisesArticlesWaitingForTheirReadMarkingToBeReversed(): void {
		$topicId = $this->store->saveTopic(['name' => 'AI', 'description' => 'AI model releases',
			'enabled' => true, 'all_feeds' => true]);
		$matched = new FreshRSS_Entry(4, 'matched', 'Matched', '', 'Body', 'https://example.com/a', 1_700_000_000);
		$matched->_id('100');
		$this->store->enqueue($matched, 2, 'pipeline', 10, archive: true);
		$claimed = $this->store->claim(600);
		self::assertNotNull($claimed);
		$this->store->addMatch($topicId, $claimed, 'Feed', 'Event', 1_700_000_000, 'Why', [1.0], null);
		self::assertTrue($this->store->completeCurrent($claimed));

		$other = new FreshRSS_Entry(4, 'other', 'Other', '', 'Body', 'https://example.com/b', 1_700_000_500);
		$other->_id('200');
		$this->store->enqueue($other, 2, 'pipeline', 10, archive: true);

		$this->store->rebuildDigests();
		$this->store->prepareRebuildJobs('pipeline-two');

		// '200' is newer, so it would win the normal published_at ordering; the pending restore must outrank it,
		// or a restart takes the whole backlog to undo the read-marking it is supposed to be undoing.
		$job = $this->store->claim(600);
		self::assertNotNull($job);
		self::assertSame('100', $job['entry_id']);
	}

	public function testAJobHandedBackBecauseThePipelineMovedOnStaysClaimableWithoutFailing(): void {
		$entry = new FreshRSS_Entry(4, 'guid', 'Model released', '', 'Detailed release announcement.',
			'https://example.com/model', 1_700_000_000);
		$entry->_id('1700000000000000');
		$this->store->enqueue($entry, 2, 'pipeline');
		$job = $this->store->claim(600);
		self::assertNotNull($job);
		self::assertSame(0, (int)$job['attempts']);

		self::assertTrue($this->store->releaseCurrent($job));
		self::assertFalse($this->store->releaseCurrent($job), 'A released job is no longer the current claim.');

		// Without this the row stays 'processing' until its lease expires, and four such expiries fail it outright.
		$reclaimed = $this->store->claim(600);
		self::assertNotNull($reclaimed);
		self::assertSame('1700000000000000', $reclaimed['entry_id']);
		self::assertSame(0, (int)$reclaimed['attempts']);
		self::assertSame('', $reclaimed['error']);
	}

	public function testRequeuingBackfillsAGuidMissingFromAnAlreadyQueuedJob(): void {
		$entry = new FreshRSS_Entry(4, 'stable-guid', 'Model released', '', 'Detailed release announcement.',
			'https://example.com/model', 1_700_000_000);
		$entry->_id('1700000000000000');
		$this->store->enqueue($entry, 2, 'pipeline');
		// Simulates a row queued before the guid column existed: every later enqueue takes the "already current"
		// early return, so unless that path backfills the guid the row can never recover a stale entry id.
		$raw = new PDO('sqlite:' . $this->databasePath);
		$raw->exec('PRAGMA busy_timeout = 5000');
		$raw->exec("UPDATE jobs SET guid=''");

		self::assertFalse($this->store->enqueue($entry, 2, 'pipeline'));

		$job = $this->store->claim(600);
		self::assertNotNull($job);
		self::assertSame('stable-guid', $job['guid']);
	}

	public function testWholeEventRestoreBlocksFingerprintAndSuggestionNeedsApproval(): void {
		$topicId = $this->store->saveTopic(['name' => 'AI', 'description' => 'AI model releases',
			'enabled' => true, 'all_feeds' => true]);
		$result = $this->store->addMatch($topicId, $this->job('100', 'Report'), 'Feed', 'Model X released',
			1_700_000_000, 'Release', [1.0], null);
		self::assertSame(['100'], $this->store->restoreEvent($topicId, $result['event_id']));
		$fingerprint = hash('sha256', mb_strtolower('Model X released', 'UTF-8'));
		self::assertTrue($this->store->isRejected($topicId, 'different', $fingerprint));
		self::assertSame('Model X released', $this->store->rejectedEventCandidates($topicId)[0]['title']);

		$suggestion = $this->store->suggestions($topicId)[0];
		$this->store->resolveSuggestion($topicId, (int)$suggestion['id'], true);
		self::assertCount(1, $this->store->topic($topicId)['exclusions']);
	}

	public function testSuggestionRecordsTheRestoredArticleAndCanBeApprovedWithEditedWording(): void {
		$topicId = $this->store->saveTopic(['name' => 'AI', 'description' => 'AI model releases',
			'enabled' => true, 'all_feeds' => true]);
		$this->store->addMatch($topicId, $this->job('100', 'Safety fears as scientists make first viruses designed by AI'),
			'Feed', 'Designed viruses', 1_700_000_000, 'Why', [1.0], null);
		$this->store->restoreSource($topicId, '100');

		$suggestions = $this->store->suggestions($topicId);
		self::assertCount(1, $suggestions);
		self::assertSame('Safety fears as scientists make first viruses designed by AI', $suggestions[0]['source_title']);
		self::assertSame('https://example.com/100', $suggestions[0]['source_link']);
		self::assertStringContainsString('Safety fears', (string)$suggestions[0]['text']);

		$this->store->resolveSuggestion($topicId, (int)$suggestions[0]['id'], true, '  Synthetic biology  ');
		self::assertSame(['Synthetic biology'], $this->store->topic($topicId)['exclusions']);
		self::assertSame([], $this->store->suggestions($topicId));
	}

	public function testApprovingASuggestionKeepsTheOriginalWordingWhenNoEditIsGiven(): void {
		$topicId = $this->store->saveTopic(['name' => 'AI', 'description' => 'AI model releases',
			'enabled' => true, 'all_feeds' => true]);
		$this->store->addMatch($topicId, $this->job('100', 'Some headline'), 'Feed', 'Event',
			1_700_000_000, 'Why', [1.0], null);
		$this->store->restoreSource($topicId, '100');
		$suggestionId = (int)$this->store->suggestions($topicId)[0]['id'];

		$this->store->resolveSuggestion($topicId, $suggestionId, true);
		self::assertSame(['Exclude items like: Some headline'], $this->store->topic($topicId)['exclusions']);
	}

	public function testAnApprovedSuggestionCannotBeEmptied(): void {
		$topicId = $this->store->saveTopic(['name' => 'AI', 'description' => 'AI model releases',
			'enabled' => true, 'all_feeds' => true]);
		$this->store->addMatch($topicId, $this->job('100', 'Some headline'), 'Feed', 'Event',
			1_700_000_000, 'Why', [1.0], null);
		$this->store->restoreSource($topicId, '100');
		$suggestionId = (int)$this->store->suggestions($topicId)[0]['id'];

		$this->expectException(InvalidArgumentException::class);
		$this->store->resolveSuggestion($topicId, $suggestionId, true, '   ');
	}

	public function testRestoringAWholeEventKeepsAnArticleToShowWithTheSuggestion(): void {
		$topicId = $this->store->saveTopic(['name' => 'AI', 'description' => 'AI model releases',
			'enabled' => true, 'all_feeds' => true]);
		$first = $this->store->addMatch($topicId, $this->job('100', 'Older coverage'), 'Feed', 'Event',
			1_700_000_000, 'Why', [1.0], null);
		$this->store->addMatch($topicId, [...$this->job('200', 'Newer coverage'), 'published_at' => 1_700_009_000],
			'Feed', 'Event', 1_700_000_000, 'Why', [1.0], $first['event_id']);

		$this->store->restoreEvent($topicId, $first['event_id']);

		// Captured before the sources are deleted, and the newest of them is the recognisable one to show.
		$suggestion = $this->store->suggestions($topicId)[0];
		self::assertSame('Newer coverage', $suggestion['source_title']);
		self::assertSame('https://example.com/200', $suggestion['source_link']);
	}

	public function testExhaustedRetriesAreDistinguishedFromARetryableBackoff(): void {
		// Only exhaustion means nothing will ever process the job again, which is what lets the worker give up on
		// an article's pending rebuild-restore and hand it back to the unread stream instead of leaving it read.
		foreach ([['100', 0, false], ['200', 3, true]] as [$entryId, $attempts, $exhausted]) {
			$entry = new FreshRSS_Entry(4, 'guid-' . $entryId, 'Article ' . $entryId, '', 'Body',
				'https://example.com/' . $entryId, 1_700_000_000);
			$entry->_id($entryId);
			$this->store->enqueue($entry, 2, 'pipeline');
			$job = $this->store->claim(600);
			self::assertNotNull($job);
			self::assertSame($entryId, $job['entry_id'], 'The previous job must be backing off, not claimable.');

			self::assertTrue($this->store->failCurrent([...$job, 'attempts' => $attempts], 'Ollama request failed.'));
			self::assertSame($exhausted, $this->store->hasExhaustedRetries($entryId));
		}
	}

	public function testStatusSeparatesRetriesFromPermanentFailuresAndClassifiedFromSkipped(): void {
		$enqueue = function (string $entryId): array {
			$entry = new FreshRSS_Entry(4, 'g-' . $entryId, 'Article ' . $entryId, '', 'Body',
				'https://example.com/' . $entryId, 1_700_000_000);
			$entry->_id($entryId);
			$this->store->enqueue($entry, 2, 'pipeline');
			$claimed = $this->store->claim(600);
			self::assertNotNull($claimed);
			return $claimed;
		};
		$this->store->failCurrent([...$enqueue('100'), 'attempts' => 0], 'Ollama request failed.');
		$this->store->failCurrent([...$enqueue('200'), 'attempts' => 3], 'Ollama request failed.');
		$this->store->completeCurrent($enqueue('300'), 'skipped', 'Entry no longer exists.');
		$this->store->completeCurrent($enqueue('400'), 'skipped', 'Entry no longer exists.');
		$this->store->completeCurrent($enqueue('500'), 'skipped', 'Marked read by a FreshRSS filter.');
		$this->store->completeCurrent($enqueue('600'), 'done');

		$status = $this->store->status();
		self::assertSame(1, $status['failed'], '"Failed" must only ever mean retries exhausted.');
		self::assertSame(1, $status['retrying'], 'A job erroring but still retryable is otherwise invisible.');
		self::assertSame(1, $status['queued'], 'It is still queue work.');
		self::assertSame(3, $status['skipped']);
		self::assertSame(1, $status['done']);
		self::assertSame(4, $status['processed'], 'Which together still make up "processed".');

		self::assertSame([
			['reason' => 'Entry no longer exists.', 'count' => 2],
			['reason' => 'Marked read by a FreshRSS filter.', 'count' => 1],
		], $this->store->skipReasons());

		$errored = array_column($this->store->recentErrors(), 'entry_id');
		sort($errored);
		self::assertSame(['100', '200'], $errored, 'Skips are intentional and stay out of the error list.');
	}

	public function testClearingErrorsKeepsTheReasonASkippedJobWasSkipped(): void {
		$entry = new FreshRSS_Entry(4, 'g', 'Filtered', '', 'Body', 'https://example.com/a', 1_700_000_000);
		$entry->_id('100');
		$this->store->enqueue($entry, 2, 'pipeline');
		$this->store->completeCurrent($this->store->claim(600) ?? [], 'skipped', 'Marked read by a FreshRSS filter.');

		$failing = new FreshRSS_Entry(4, 'g2', 'Broken', '', 'Body', 'https://example.com/b', 1_700_000_000);
		$failing->_id('200');
		$this->store->enqueue($failing, 2, 'pipeline');
		$this->store->failCurrent([...$this->store->claim(600) ?? [], 'attempts' => 3], 'Ollama request failed.');

		// A skipped job's "error" is the recorded reason for a settled outcome, not a failure to be cleared.
		self::assertSame(1, $this->store->clearErrors());
		self::assertSame([['reason' => 'Marked read by a FreshRSS filter.', 'count' => 1]], $this->store->skipReasons());
	}

	public function testMigrationDiscardsUnresolvableLegacyJobsButKeepsRealDeletions(): void {
		$raw = new PDO('sqlite:' . $this->databasePath);
		$raw->exec('PRAGMA busy_timeout = 5000');
		$raw->exec("INSERT INTO jobs(entry_id,feed_id,category_id,title,author,link,published_at,content_hash,"
			. "pipeline_hash,rss_text,state,error,guid,created_at,updated_at) VALUES"
			. "('1',1,1,'Renumbered','','',0,'','','','skipped','Entry no longer exists.','',0,0),"
			. "('2',1,1,'Really gone','','',0,'','','','skipped','Entry no longer exists.','real-guid',0,0),"
			. "('3',1,1,'Filtered','','',0,'','','','skipped','Marked read by a FreshRSS filter.','',0,0),"
			. "('4',1,1,'Classified','','',0,'','','','done','','',0,0)");
		// Rewind the recorded version so the migration runs against this as against a real installation.
		$raw->exec('PRAGMA user_version = 8');
		unset($raw);

		$migrated = new TopicDigestStore($this->databasePath);

		// Row 1 was never a deleted article: it is keyed by an id FreshRSS renumbered at commit time, with no
		// GUID left to resolve it by. Row 2 has a GUID, so its disappearance is a real deletion.
		self::assertSame([
			['reason' => 'Entry no longer exists.', 'count' => 1],
			['reason' => 'Marked read by a FreshRSS filter.', 'count' => 1],
		], $migrated->skipReasons());
		self::assertSame(1, $migrated->status()['done'], 'Unrelated jobs are untouched.');
	}

	public function testMigrationAlsoDiscardsArtefactsWhoseReasonWasErasedByResetLogs(): void {
		$raw = new PDO('sqlite:' . $this->databasePath);
		$raw->exec('PRAGMA busy_timeout = 5000');
		$raw->exec("INSERT INTO jobs(entry_id,feed_id,category_id,title,author,link,published_at,content_hash,"
			. "pipeline_hash,rss_text,state,error,guid,created_at,updated_at) VALUES"
			. "('1',1,1,'Wiped reason','','',0,'','','','skipped','','',0,0),"
			. "('2',1,1,'Really gone','','',0,'','','','skipped','Entry no longer exists.','a-guid',0,0)");
		$raw->exec("PRAGMA user_version = 9");
		unset($raw);

		// An archive rescan can never reach a row whose id matches no live article, so an unresolvable one left
		// behind sits in the totals permanently as an unaccountable "(no reason recorded)".
		$migrated = new TopicDigestStore($this->databasePath);
		self::assertSame([['reason' => 'Entry no longer exists.', 'count' => 1]], $migrated->skipReasons());
	}

	public function testSkippedArticlesCanBeRequeuedForAFreshDecision(): void {
		foreach ([['100', 'Marked read by a FreshRSS filter.'], ['200', 'No active topic includes this article.']]
				as [$entryId, $reason]) {
			$entry = new FreshRSS_Entry(4, 'g-' . $entryId, 'Article ' . $entryId, '', 'Body',
				'https://example.com/' . $entryId, 1_700_000_000);
			$entry->_id($entryId);
			$this->store->enqueue($entry, 2, 'pipeline');
			$this->store->completeCurrent($this->store->claim(600) ?? [], 'skipped', $reason);
		}
		self::assertSame(2, $this->store->status()['skipped']);

		self::assertSame(2, $this->store->retrySkipped());
		self::assertSame(0, $this->store->status()['skipped']);
		self::assertSame(2, $this->store->status()['queued']);
		self::assertSame([], $this->store->skipReasons());
		self::assertSame(0, $this->store->retrySkipped(), 'Nothing left to re-queue.');

		$job = $this->store->claim(600);
		self::assertNotNull($job);
		self::assertSame(0, (int)$job['attempts'], 'Re-queued with a clean slate, not a used-up retry budget.');
	}

	public function testManualUnreadProtectionCanBeClearedByALaterRead(): void {
		$this->store->recordManualUnread('123');
		self::assertTrue($this->store->isProtected('123'));
		$this->store->clearManualUnread('123');
		self::assertFalse($this->store->isProtected('123'));
	}

	public function testGlobalPauseStateIsReportedAndPreventsClaims(): void {
		$entry = new FreshRSS_Entry(4, 'guid', 'Model released', '', 'Detailed release announcement.',
			'https://example.com/model', 1_700_000_000);
		$entry->_id('1700000000000000');
		$this->store->enqueue($entry, 2, 'pipeline', grace: 0);
		$this->store->setPaused(true);

		self::assertTrue($this->store->isPaused());
		self::assertSame(1, $this->store->status()['paused']);
		self::assertNull($this->store->claim(600));

		$this->store->setPaused(false);
		self::assertFalse($this->store->isPaused());
		self::assertNotNull($this->store->claim(600));
	}

	public function testStaleQueueRowsArePrunedWithoutTouchingResolvableWork(): void {
		// A row queued before the GUID column existed, keyed by an id FreshRSS has since renumbered.
		$stale = new FreshRSS_Entry(4, '', 'Model released', '', 'Detailed release announcement.',
			'https://example.com/stale', 1_700_000_000);
		$stale->_id('1700000000000001');
		$resolvable = new FreshRSS_Entry(4, 'guid', 'Model released', '', 'Detailed release announcement.',
			'https://example.com/live', 1_700_000_000);
		$resolvable->_id('1700000000000002');
		$this->store->enqueue($stale, 2, 'pipeline');
		$this->store->enqueue($resolvable, 2, 'pipeline');

		self::assertSame(1, $this->store->stalePendingJobCount());
		self::assertSame(1, $this->store->pruneStalePendingJobs());
		self::assertSame(1, $this->store->staleJobsDiscarded());
		self::assertSame(0, $this->store->stalePendingJobCount());
		// The row that can still be resolved is left queued.
		self::assertSame(1, (int)$this->store->status()['queued']);
	}

	public function testAlreadyRecordedStaleSkipsAreFoldedIntoOneTotal(): void {
		$stale = new FreshRSS_Entry(4, '', 'Model released', '', 'Detailed release announcement.',
			'https://example.com/stale', 1_700_000_000);
		$stale->_id('1700000000000001');
		$this->store->enqueue($stale, 2, 'pipeline');
		$job = $this->store->claim(600);
		self::assertNotNull($job);
		self::assertTrue($this->store->completeCurrent($job, 'skipped', TopicDigestStore::STALE_JOB_REASON));
		self::assertSame(1, (int)$this->store->status()['processed']);
		// Simulate opening a database created by 0.5.1 so the versioned cleanup is exercised rather than the
		// current-schema fast path.
		$raw = new PDO('sqlite:' . $this->databasePath);
		$raw->exec('PRAGMA user_version = 10');

		// Opening the store again runs the cleanup: these were never articles, so they stop being counted as
		// articles that were processed and skipped, and stop crowding out the real skip reasons.
		$reopened = new TopicDigestStore($this->databasePath);
		self::assertSame(0, (int)$reopened->status()['processed']);
		self::assertSame(1, $reopened->discardedSkipsOnOpen());
		self::assertSame(1, $reopened->staleJobsDiscarded());
		self::assertSame(1, (int)$reopened->status()['stale_discarded']);
		self::assertSame([], $reopened->skipReasons());
	}

	public function testNewStaleQueueRowsAreDiscardedInsteadOfRecordedAsProcessed(): void {
		$stale = new FreshRSS_Entry(4, '', 'Model released', '', 'Detailed release announcement.',
			'https://example.com/stale', 1_700_000_000);
		$stale->_id('1700000000000001');
		$this->store->enqueue($stale, 2, 'pipeline');
		$job = $this->store->claim(600);
		self::assertNotNull($job);

		self::assertTrue($this->store->discardStaleCurrent($job));
		self::assertSame(0, (int)$this->store->status()['processed']);
		self::assertSame(0, (int)$this->store->status()['queued']);
		self::assertSame(1, $this->store->staleJobsDiscarded());
	}

	public function testCloudUnavailabilityAndConfiguredProfileAreTracked(): void {
		self::assertSame(0, $this->store->cloudUnavailableUntil());
		self::assertNull($this->store->lastOllamaProfile());

		$this->store->markCloudUnavailable(1_700_000_900);
		self::assertSame(1_700_000_900, $this->store->cloudUnavailableUntil());

		$this->store->setLastOllamaProfile('local');
		self::assertSame('local', $this->store->lastOllamaProfile());
	}

	public function testFilterReadEntriesAreTrackedAndIdempotent(): void {
		self::assertFalse($this->store->isFilterRead('100'));
		$this->store->markFilterRead('100');
		self::assertTrue($this->store->isFilterRead('100'));
		self::assertFalse($this->store->isFilterRead('101'));
		$this->store->markFilterRead('100');
		self::assertTrue($this->store->isFilterRead('100'));
	}

	public function testStaleEntryIdCanBeRekeyedAfterGuidResolution(): void {
		$entry = new FreshRSS_Entry(4, 'stable-guid', 'Model released', '', 'Detailed release announcement.',
			'https://example.com/model', 1_700_000_000);
		$entry->_id('1700000000000000');
		self::assertTrue($this->store->enqueue($entry, 2, 'pipeline'));
		$job = $this->store->claim(600);
		self::assertNotNull($job);
		self::assertSame('stable-guid', $job['guid']);
		self::assertTrue($this->store->isCurrentJob($job));

		self::assertTrue($this->store->rekeyJob('1700000000000000', '1700000000000099'));
		self::assertFalse($this->store->isCurrentJob($job));
		$rekeyedJob = ['entry_id' => '1700000000000099', 'content_hash' => $job['content_hash'],
			'pipeline_hash' => $job['pipeline_hash']];
		self::assertTrue($this->store->isCurrentJob($rekeyedJob));
	}

	public function testAverageSpeedUsesOnlyRecordedActiveProcessingTime(): void {
		self::assertSame(0, $this->store->status()['average_ready']);
		self::assertSame(0.0, $this->store->status()['average_per_hour']);
		foreach ([60.0, 60.0, 60.0] as $index => $seconds) {
			$entry = new FreshRSS_Entry(4, 'metric-' . $index, 'Article ' . $index, '', 'Article body',
				'https://example.com/metric-' . $index, 1_700_000_000 + $index);
			$entry->_id((string)(1_800_000_000_000_000 + $index));
			$this->store->enqueue($entry, 2, 'pipeline', grace: 0);
			$job = $this->store->claim(600);
			self::assertNotNull($job);
			self::assertTrue($this->store->completeCurrent($job));
			self::assertTrue($this->store->recordProcessingActivity($entry->id(), $seconds));
		}
		$status = $this->store->status();

		self::assertSame(180.0, $status['active_processing_seconds']);
		self::assertSame(3, $status['active_processed_articles']);
		self::assertSame(1, $status['average_ready']);
		self::assertSame(60.0, $status['average_per_hour']);

		$this->store->setPaused(true);
		self::assertSame(60.0, $this->store->status()['average_per_hour']);
	}

	public function testSharedSummaryCacheRequiresMatchingContentAndModels(): void {
		$path = sys_get_temp_dir() . '/freshrss-article-summaries-' . bin2hex(random_bytes(8)) . '.sqlite';
		try {
			$cache = new FreshRSS_ArticleSummaryCache($path);
			$cache->save('123', 'hash', 'summary-model', 'embedding-model', 'A model was released.',
				'Article text', [0.5, 0.25], 'Model released', '2026-08-27', 'topic_digest');
			$summary = $cache->find('123', 'hash', 'summary-model', 'embedding-model');
			self::assertSame('A model was released.', $summary['summary_text'] ?? null);
			self::assertSame([0.5, 0.25], $summary['embedding'] ?? null);
			self::assertNull($cache->find('123', 'changed-hash', 'summary-model', 'embedding-model'));
			self::assertNull($cache->find('123', 'hash', 'different-model', 'embedding-model'));
		} finally {
			unset($cache);
			foreach ([$path, $path . '-wal', $path . '-shm'] as $file) {
				if (file_exists($file)) {
					unlink($file);
				}
			}
		}
	}

	public function testStoredErrorsCanBeClearedWithoutChangingTheJobState(): void {
		$entry = new FreshRSS_Entry(4, 'guid', 'Model released', '', 'Detailed release announcement.',
			'https://example.com/model', 1_700_000_000);
		$entry->_id('1700000000000000');
		$this->store->enqueue($entry, 2, 'pipeline', grace: 0);
		$job = $this->store->claim(600);
		self::assertNotNull($job);
		self::assertTrue($this->store->failCurrent($job, 'Connection failed.'));
		self::assertCount(1, $this->store->recentErrors());

		self::assertSame(1, $this->store->clearErrors());
		self::assertSame([], $this->store->recentErrors());
		self::assertSame(1, $this->store->status()['pending']);
	}

	public function testChangedSourceIsDetachedBeforeReclassification(): void {
		$topicId = $this->store->saveTopic(['name' => 'AI', 'description' => 'AI model releases',
			'enabled' => true, 'all_feeds' => true]);
		$this->store->addMatch($topicId, $this->job('100', 'Old report'), 'Feed', 'Model X released',
			1_700_000_000, 'Release', [1.0], null);
		self::assertSame([$topicId], $this->store->detachChangedSources('100', hash('md5', 'New report')));
		self::assertSame([], $this->store->events($topicId));
	}

	public function testRuleReclassificationCanRemoveMembershipWithoutCreatingARejection(): void {
		$topicId = $this->store->saveTopic(['name' => 'AI', 'description' => 'AI model releases',
			'enabled' => true, 'all_feeds' => true]);
		$this->store->addMatch($topicId, $this->job('100', 'Report'), 'Feed', 'Model X released',
			1_700_000_000, 'Release', [1.0], null);
		self::assertSame([$topicId], $this->store->topicIdsForSource('100'));
		self::assertTrue($this->store->removeSourceMembership($topicId, '100'));
		self::assertFalse($this->store->isRejected($topicId, '100'));
	}

	public function testFirstSourceOfAnEventIsRecordedAsItsPrimary(): void {
		$topicId = $this->store->saveTopic(['name' => 'AI', 'description' => 'AI model releases',
			'enabled' => true, 'all_feeds' => true, 'topic_type' => 'feed']);
		$first = $this->store->addMatch($topicId, $this->job('100', 'Outlet A'), 'FeedA', 'Model X released',
			1_700_000_000, 'Release', [1.0], null);
		$second = $this->store->addMatch($topicId, $this->job('200', 'Outlet B'), 'FeedB', 'Model X released',
			1_700_000_100, 'Also released', [1.0], $first['event_id']);
		self::assertTrue($first['new_event']);
		self::assertFalse($second['new_event']);
		self::assertSame($first['event_id'], $second['event_id']);

		$events = $this->store->events($topicId);
		self::assertCount(1, $events);
		self::assertSame('100', $events[0]['primary_entry_id']);
		self::assertCount(2, $events[0]['sources']);
	}

	public function testRemovingTheEventPrimaryPromotesTheRemainingSource(): void {
		$topicId = $this->store->saveTopic(['name' => 'AI', 'description' => 'AI model releases',
			'enabled' => true, 'all_feeds' => true, 'topic_type' => 'feed']);
		$first = $this->store->addMatch($topicId, $this->job('100', 'Outlet A'), 'FeedA', 'Model X released',
			1_700_000_000, 'Release', [1.0], null);
		$this->store->addMatch($topicId, $this->job('200', 'Outlet B'), 'FeedB', 'Model X released',
			1_700_000_100, 'Also released', [1.0], $first['event_id']);

		self::assertTrue($this->store->removeSourceMembership($topicId, '100'));
		$events = $this->store->events($topicId);
		self::assertCount(1, $events);
		self::assertSame('200', $events[0]['primary_entry_id']);
		self::assertCount(1, $events[0]['sources']);

		self::assertTrue($this->store->removeSourceMembership($topicId, '200'));
		self::assertSame([], $this->store->events($topicId));
	}

	public function testRemovingANonPrimarySourceLeavesThePrimaryUnchanged(): void {
		$topicId = $this->store->saveTopic(['name' => 'AI', 'description' => 'AI model releases',
			'enabled' => true, 'all_feeds' => true, 'topic_type' => 'feed']);
		$first = $this->store->addMatch($topicId, $this->job('100', 'Outlet A'), 'FeedA', 'Model X released',
			1_700_000_000, 'Release', [1.0], null);
		$this->store->addMatch($topicId, $this->job('200', 'Outlet B'), 'FeedB', 'Model X released',
			1_700_000_100, 'Also released', [1.0], $first['event_id']);

		self::assertTrue($this->store->removeSourceMembership($topicId, '200'));
		$events = $this->store->events($topicId);
		self::assertCount(1, $events);
		self::assertSame('100', $events[0]['primary_entry_id']);
		self::assertCount(1, $events[0]['sources']);
	}

	public function testMatchedSourceEntryIdsListsEveryDistinctMatchedEntry(): void {
		$topicId = $this->store->saveTopic(['name' => 'AI', 'description' => 'AI model releases',
			'enabled' => true, 'all_feeds' => true]);
		$otherTopicId = $this->store->saveTopic(['name' => 'Privacy', 'description' => 'Privacy news',
			'enabled' => true, 'all_feeds' => true]);
		self::assertSame([], $this->store->matchedSourceEntryIds());

		$this->store->addMatch($topicId, $this->job('100', 'Model released'), 'FeedA', 'Model X released',
			1_700_000_000, 'Release', [1.0], null);
		$this->store->addMatch($otherTopicId, $this->job('200', 'Leak disclosed'), 'FeedB', 'Leak Y',
			1_700_000_100, 'Leak', [1.0], null);
		self::assertSame(['100', '200'], $this->store->matchedSourceEntryIds());
	}

	public function testHasMatchingSourceContentHashDetectsChangedOrUnknownEntries(): void {
		$topicId = $this->store->saveTopic(['name' => 'AI', 'description' => 'AI model releases',
			'enabled' => true, 'all_feeds' => true]);
		$job = $this->job('100', 'Model released');
		$this->store->addMatch($topicId, $job, 'FeedA', 'Model X released', 1_700_000_000, 'Release', [1.0], null);

		self::assertTrue($this->store->hasMatchingSourceContentHash('100', $job['content_hash']));
		self::assertFalse($this->store->hasMatchingSourceContentHash('100', hash('md5', 'Something else now')));
		self::assertFalse($this->store->hasMatchingSourceContentHash('999', $job['content_hash']));
	}

	public function testOllamaDropsAdditionalStructuredFieldsInsteadOfLosingTheAnswer(): void {
		// On an endpoint that cannot enforce the schema, refusing a complete answer over one surplus key only
		// loses the article. The extra key is dropped so no caller ever sees an undeclared field.
		$transport = static fn(string $method, string $path, ?array $payload): array => [
			'message' => ['content' => json_encode([
				'summary' => 'Summary', 'event_title' => 'Title', 'event_date' => '2026-01-01', 'extra' => 'ignored',
			], JSON_THROW_ON_ERROR)],
		];
		$ollama = new TopicDigestOllama('http://ollama', 10, $transport);
		self::assertSame(['summary' => 'Summary', 'event_title' => 'Title', 'event_date' => '2026-01-01'],
			$ollama->summarise('model', 'Title', 'Article', 1_700_000_000));
	}

	public function testOllamaFallsBackToTheTitleOnlyForAHeadlineOnlyEntry(): void {
		$empty = static fn(string $method, string $path, ?array $payload): array => [
			'message' => ['content' => json_encode(['summary' => '', 'event_title' => '  ', 'event_date' => ''],
				JSON_THROW_ON_ERROR)]];
		$ollama = new TopicDigestOllama('http://ollama', 10, $empty);

		// A video post or link-only feed entry genuinely has nothing to summarise; failing it four times over is
		// worse than classifying it on the one fact it carries.
		self::assertSame(['summary' => 'Baiting the fans', 'event_title' => 'Baiting the fans', 'event_date' => ''],
			$ollama->summarise('model', 'Baiting the fans', '', 1_700_000_000));

		// But an empty reply about an article with real text is a model failure worth retrying. Classifying that
		// on its headline alone would turn a visible error into an invisible guess.
		try {
			$ollama->summarise('model', 'Real headline', str_repeat('Substantial article text. ', 60), 1_700_000_000);
			self::fail('An empty summary for a full article must remain an error.');
		} catch (RuntimeException $e) {
			self::assertStringContainsString('characters of text', $e->getMessage());
		}
	}

	public function testOllamaRequiresNonEmptySummaryFieldsDuringStructuredDecoding(): void {
		$payload = null;
		$transport = static function (string $method, string $path, ?array $request) use (&$payload): array {
			$payload = $request;
			return ['message' => ['content' => json_encode(['summary' => 'Summary', 'event_title' => 'Event',
				'event_date' => '2026-08-29'], JSON_THROW_ON_ERROR)]];
		};
		$ollama = new TopicDigestOllama('http://ollama', 10, $transport);
		$ollama->summarise('model', 'Title', 'Substantial article text.', 1_700_000_000);

		self::assertIsArray($payload);
		self::assertSame(1, $payload['format']['properties']['summary']['minLength'] ?? null);
		self::assertSame(1, $payload['format']['properties']['event_title']['minLength'] ?? null);
		self::assertStringContainsString('must each contain non-whitespace text',
			(string)($payload['messages'][0]['content'] ?? ''));
	}

	public function testOllamaImmediatelyCorrectsAnEmptySummaryForASubstantialArticle(): void {
		$calls = 0;
		$secondSystemPrompt = '';
		$transport = static function (string $method, string $path, ?array $request)
				use (&$calls, &$secondSystemPrompt): array {
			$calls++;
			if ($calls === 1) {
				return ['message' => ['content' => json_encode(['summary' => '', 'event_title' => '',
					'event_date' => ''], JSON_THROW_ON_ERROR)]];
			}
			$secondSystemPrompt = (string)($request['messages'][0]['content'] ?? '');
			return ['message' => ['content' => json_encode(['summary' => 'Extreme heat is linked to 33 conditions.',
				'event_title' => 'Study links heat to 33 conditions', 'event_date' => '2026-08-29'],
				JSON_THROW_ON_ERROR)]];
		};
		$ollama = new TopicDigestOllama('http://ollama', 10, $transport);
		$result = $ollama->summarise('model', 'Beyond heatstroke', str_repeat('Substantial article text. ', 60),
			1_700_000_000);

		self::assertSame(2, $calls);
		self::assertSame('Extreme heat is linked to 33 conditions.', $result['summary']);
		self::assertStringContainsString('previous attempt returned an empty summary', $secondSystemPrompt);
	}

	public function testOllamaStillRejectsAReplyMissingARequiredField(): void {
		$transport = static fn(string $method, string $path, ?array $payload): array => [
			'message' => ['content' => json_encode(['summary' => 'Summary', 'event_title' => 'Title'], JSON_THROW_ON_ERROR)],
		];
		$ollama = new TopicDigestOllama('http://ollama', 10, $transport);
		$this->expectException(RuntimeException::class);
		$ollama->summarise('model', 'Title', 'Article', 1_700_000_000);
	}

	public function testOllamaSpellsTheRequiredSchemaOutInThePromptItself(): void {
		// Ollama Cloud ignores the "format" schema constraint entirely, so a schema sent only in that field is
		// never seen by the model, which then invents its own key names.
		$system = '';
		$transport = static function (string $method, string $path, ?array $payload) use (&$system): array {
			$system = $payload['messages'][0]['content'] ?? '';
			return ['message' => ['content' => json_encode(['summary' => 'S', 'event_title' => 'T',
				'event_date' => '2026-01-01'], JSON_THROW_ON_ERROR)]];
		};
		$ollama = new TopicDigestOllama('http://ollama', 10, $transport);
		$ollama->summarise('model', 'Title', 'Article', 1_700_000_000);

		self::assertStringContainsString('"event_title"', $system, 'The schema itself must be in the prompt.');
		self::assertStringContainsString('summary, event_title, event_date', $system,
			'The exact key names must be spelled out in the prompt.');
	}

	public function testOllamaThrowsWhenPrimaryReturnsProseAndNoStructuringFallbackConfigured(): void {
		$transport = static fn(string $method, string $path, ?array $payload): array => [
			'message' => ['content' => '**Event Title:** Something happened **Summary:** Prose, not JSON.'],
			'done_reason' => 'stop',
		];
		$ollama = new TopicDigestOllama('http://ollama-cloud', 10, $transport);
		$this->expectException(RuntimeException::class);
		$ollama->summarise('cloud-model', 'Title', 'Article', 1_700_000_000);
	}

	public function testOllamaRecoversStructuredReplyViaLocalStructuringFallback(): void {
		$calls = 0;
		$transport = static function (string $method, string $path, ?array $payload) use (&$calls): array {
			$calls++;
			if ($calls === 1) {
				return ['message' => ['content' => '**Event Title:** Something happened **Summary:** Prose, not JSON.'],
					'done_reason' => 'stop'];
			}
			self::assertSame('structuring-model', $payload['model'] ?? null);
			return ['message' => ['content' => json_encode(['summary' => 'Prose, not JSON.',
				'event_title' => 'Something happened', 'event_date' => '2026-01-01'], JSON_THROW_ON_ERROR)]];
		};
		$ollama = new TopicDigestOllama('http://ollama-cloud', 10, $transport,
			structuringUrl: 'http://ollama-local', structuringModel: 'structuring-model');
		$result = $ollama->summarise('cloud-model', 'Title', 'Article', 1_700_000_000);
		self::assertSame(2, $calls);
		self::assertSame('Something happened', $result['event_title']);
	}

	public function testOllamaBatchesTopicDecisionsInOneRequest(): void {
		$calls = 0;
		$transport = static function (string $method, string $path, ?array $payload) use (&$calls): array {
			$calls++;
			self::assertSame('/api/chat', $path);
			self::assertIsArray($payload);
			self::assertStringContainsString('semantic domain', (string)$payload['messages'][0]['content']);
			$request = json_decode((string)$payload['messages'][1]['content'], true, flags: JSON_THROW_ON_ERROR);
			self::assertIsArray($request);
			self::assertCount(2, $request['topics']);
			self::assertSame('AI releases', $request['topics'][0]['name']);
			return ['message' => ['content' => json_encode(['decisions' => [
				['topic_id' => 2, 'matches' => false, 'confidence' => 0.1, 'reason' => '', 'event_title' => ''],
				['topic_id' => 1, 'matches' => true, 'confidence' => 0.95,
					'reason' => 'A release was announced.', 'event_title' => 'Model released'],
			]], JSON_THROW_ON_ERROR)]];
		};
		$ollama = new TopicDigestOllama('http://ollama', 10, $transport);
		$result = $ollama->matchTopics('model', ['summary' => 'A model was released.'], [
			['id' => 1, 'name' => 'AI releases', 'description' => 'New model releases', 'exclusions' => []],
			['id' => 2, 'name' => 'Physics', 'description' => 'New experimental results', 'exclusions' => []],
		]);
		self::assertSame(1, $calls);
		self::assertTrue($result[1]['matches']);
		self::assertFalse($result[2]['matches']);
	}

	public function testOllamaReshapesAFlatBooleanMapIntoTopicDecisions(): void {
		$transport = static fn(string $method, string $path, ?array $payload): array => [
			'message' => ['content' => json_encode(['1' => true, '2' => false], JSON_THROW_ON_ERROR)],
			'done_reason' => 'stop',
		];
		$ollama = new TopicDigestOllama('http://ollama', 10, $transport);
		$result = $ollama->matchTopics('model', ['summary' => 'A model was released.', 'event_title' => 'Model released'], [
			['id' => 1, 'name' => 'AI releases', 'description' => 'New model releases', 'exclusions' => []],
			['id' => 2, 'name' => 'Physics', 'description' => 'New experimental results', 'exclusions' => []],
		]);
		self::assertTrue($result[1]['matches']);
		self::assertSame('Model released', $result[1]['event_title']);
		self::assertFalse($result[2]['matches']);
	}

	public function testOllamaReshapesAFlatBooleanMapIntoEventDecisionsMissingKeysDefaultFalse(): void {
		$transport = static fn(string $method, string $path, ?array $payload): array => [
			'message' => ['content' => json_encode(['e:1' => true], JSON_THROW_ON_ERROR)],
			'done_reason' => 'stop',
		];
		$ollama = new TopicDigestOllama('http://ollama', 10, $transport);
		$result = $ollama->sameEvents('model', ['summary' => 'Release'], [
			['candidate_id' => 'e:1', 'title' => 'Release', 'occurred_at' => 1_700_000_000, 'explanation' => 'Release'],
			['candidate_id' => 'e:2', 'title' => 'Other', 'occurred_at' => 1_700_000_001, 'explanation' => 'Other'],
		]);
		self::assertTrue($result['e:1']['same_event']);
		self::assertFalse($result['e:2']['same_event']);
	}

	public function testOllamaReadsAWordedVerdictWrittenInPlaceOfABoolean(): void {
		// Observed from gemma4:31b:cloud, which answered {"e:402":"different"} instead of the batch schema.
		$transport = static fn(string $method, string $path, ?array $payload): array => [
			'message' => ['content' => json_encode(['e:402' => 'different', 'e:403' => 'Same Event'], JSON_THROW_ON_ERROR)],
			'done_reason' => 'stop',
		];
		$ollama = new TopicDigestOllama('http://ollama', 10, $transport);
		$result = $ollama->sameEvents('model', ['summary' => 'Release'], [
			['candidate_id' => 'e:402', 'title' => 'Release', 'occurred_at' => 1_700_000_000, 'explanation' => 'Release'],
			['candidate_id' => 'e:403', 'title' => 'Other', 'occurred_at' => 1_700_000_001, 'explanation' => 'Other'],
		]);
		self::assertFalse($result['e:402']['same_event']);
		self::assertTrue($result['e:403']['same_event']);
	}

	public function testOllamaReadsAWordedVerdictInATopicBatchButRefusesFreeText(): void {
		$reply = ['1' => 'yes', '2' => 'no'];
		$transport = static function (string $method, string $path, ?array $payload) use (&$reply): array {
			return ['message' => ['content' => json_encode($reply, JSON_THROW_ON_ERROR)], 'done_reason' => 'stop'];
		};
		$topics = [
			['id' => 1, 'name' => 'AI releases', 'description' => 'New model releases', 'exclusions' => []],
			['id' => 2, 'name' => 'Physics', 'description' => 'New experimental results', 'exclusions' => []],
		];
		$ollama = new TopicDigestOllama('http://ollama', 10, $transport);
		$result = $ollama->matchTopics('model', ['summary' => 'A model was released.', 'event_title' => 'Model released'], $topics);
		self::assertTrue($result[1]['matches']);
		self::assertFalse($result[2]['matches']);

		// A value outside the recognised vocabulary is not guessed at; the whole reply is refused.
		$reply = ['1' => 'probably relevant', '2' => 'no'];
		$this->expectException(RuntimeException::class);
		$ollama->matchTopics('model', ['summary' => 'A model was released.', 'event_title' => 'Model released'], $topics);
	}

	public function testOllamaDoesNotAskTheStructuringModelToReinterpretWronglyShapedJson(): void {
		// The structuring model cannot know what a wrongly-shaped reply meant, so it invents values and writes its
		// own confusion into "reason". Only a genuinely unstructured reply is worth sending to it.
		$calls = 0;
		$transport = static function (string $method, string $path, ?array $payload) use (&$calls): array {
			$calls++;
			return ['message' => ['content' => json_encode(['unexpected' => 'shape'], JSON_THROW_ON_ERROR)],
				'done_reason' => 'stop'];
		};
		$ollama = new TopicDigestOllama('http://ollama-cloud', 10, $transport,
			structuringUrl: 'http://ollama-local', structuringModel: 'structuring-model');
		try {
			$ollama->summarise('cloud-model', 'Title', 'Article', 1_700_000_000);
			self::fail('A wrongly-shaped reply must still be an error.');
		} catch (RuntimeException $e) {
			self::assertSame(1, $calls, 'The structuring model must not be called for decodable JSON.');
			self::assertStringNotContainsString('structuring fallback', $e->getMessage());
		}
	}

	public function testOllamaDiscardsAnEventTitleThatOnlyLabelsTheDecision(): void {
		// Observed in production: every match against topic 2 came back titled "Topic 2 Decision", which names
		// nothing about the article and collapses unrelated articles onto one event fingerprint.
		$title = 'Topic 2 Decision';
		$transport = static function (string $method, string $path, ?array $payload) use (&$title): array {
			return ['message' => ['content' => json_encode(['decisions' => [['topic_id' => 1, 'matches' => true,
				'confidence' => 0.9, 'reason' => 'It reports a release.', 'event_title' => $title]]],
				JSON_THROW_ON_ERROR)]];
		};
		$ollama = new TopicDigestOllama('http://ollama', 10, $transport);
		$topics = [['id' => 1, 'name' => 'AI releases', 'description' => 'New model releases', 'exclusions' => []]];
		$summary = ['summary' => 'GLM-5.3 was released.', 'event_title' => 'GLM-5.3 is now open-weight'];

		$result = $ollama->matchTopics('model', $summary, $topics);
		self::assertTrue($result[1]['matches'], 'The decision itself is still valid.');
		self::assertSame('', $result[1]['event_title'], 'A decision label is reported as no title at all.');

		$title = 'GLM-5.3 released as open weights';
		self::assertSame('GLM-5.3 released as open weights',
			$ollama->matchTopics('model', $summary, $topics)[1]['event_title']);

		// A real title is kept even when it contains one of the label words.
		$title = 'Chess match ends in a decision on move 40';
		self::assertSame('Chess match ends in a decision on move 40',
			$ollama->matchTopics('model', $summary, $topics)[1]['event_title']);
	}

	public function testOllamaBatchesEventDecisionsAndRejectsMissingCandidates(): void {
		$transport = static fn(string $method, string $path, ?array $payload): array => [
			'message' => ['content' => json_encode(['decisions' => [
				['candidate_id' => 'e:1', 'same_event' => true, 'confidence' => 0.9, 'reason' => 'Same release.'],
			]], JSON_THROW_ON_ERROR)],
		];
		$ollama = new TopicDigestOllama('http://ollama', 10, $transport);
		$this->expectException(RuntimeException::class);
		$ollama->sameEvents('model', ['summary' => 'Release'], [
			['candidate_id' => 'e:1', 'title' => 'Release', 'occurred_at' => 1_700_000_000, 'explanation' => 'Release'],
			['candidate_id' => 'e:2', 'title' => 'Other', 'occurred_at' => 1_700_000_001, 'explanation' => 'Other'],
		]);
	}

	public function testOllamaToleratesCandidateIdPunctuationAndCaseMangling(): void {
		$transport = static fn(string $method, string $path, ?array $payload): array => [
			'message' => ['content' => json_encode(['decisions' => [
				['candidate_id' => 'E-130', 'same_event' => false, 'confidence' => 0.95, 'reason' => 'Different event.'],
			]], JSON_THROW_ON_ERROR)],
		];
		$ollama = new TopicDigestOllama('http://ollama', 10, $transport);
		$result = $ollama->sameEvents('model', ['summary' => 'Release'], [
			['candidate_id' => 'e:130', 'title' => 'Release', 'occurred_at' => 1_700_000_000, 'explanation' => 'Release'],
		]);
		self::assertFalse($result['e:130']['same_event']);
	}

	public function testDecisionCachesRequireExactRevisions(): void {
		$topicDecision = ['matches' => true, 'confidence' => 0.95, 'reason' => 'Release',
			'event_title' => 'Model released'];
		$this->store->saveTopicDecision('100', 'content-a', 1, 'rule-a', 'judge-a', $topicDecision);
		self::assertSame($topicDecision, $this->store->topicDecision('100', 'content-a', 1, 'rule-a', 'judge-a'));
		self::assertNull($this->store->topicDecision('100', 'content-b', 1, 'rule-a', 'judge-a'));
		self::assertNull($this->store->topicDecision('100', 'content-a', 1, 'rule-b', 'judge-a'));
		self::assertNull($this->store->topicDecision('100', 'content-a', 1, 'rule-a', 'judge-b'));

		$eventDecision = ['same_event' => true, 'confidence' => 0.9, 'reason' => 'Same release'];
		$this->store->saveEventDecision('100', 'content-a', 1, 'e:1', 'candidate-a', 'judge-a', $eventDecision);
		self::assertSame($eventDecision,
			$this->store->eventDecision('100', 'content-a', 1, 'e:1', 'candidate-a', 'judge-a'));
		self::assertNull($this->store->eventDecision('100', 'content-a', 1, 'e:1', 'candidate-b', 'judge-a'));
	}

	public function testCosineSimilarityHandlesValidAndMismatchedVectors(): void {
		self::assertEqualsWithDelta(1.0, TopicDigestProcessor::cosine([1.0, 2.0], [1.0, 2.0]), 0.00001);
		self::assertSame(-1.0, TopicDigestProcessor::cosine([1.0], [1.0, 2.0]));
	}

	// -- TopicDigestOpenAICompatible ---------------------------------------------------------------------------

	public function testOpenAiCompatibleProducesStructuredSummaryViaChatCompletions(): void {
		$transport = static fn(string $method, string $url, ?array $payload, array $headers): array => [
			'choices' => [['message' => ['content' => json_encode(['summary' => 'GLM-5.3 was released.',
				'event_title' => 'GLM-5.3 released', 'event_date' => '2026-01-01'], JSON_THROW_ON_ERROR)],
				'finish_reason' => 'stop']],
		];
		$provider = new TopicDigestOpenAICompatible('https://openrouter.ai/api/v1', 'sk-test', 10, $transport);
		$result = $provider->summarise('openai/gpt-oss-20b', 'Title', 'Substantial article text.', 1_700_000_000);
		self::assertSame('GLM-5.3 was released.', $result['summary']);
		self::assertSame('GLM-5.3 released', $result['event_title']);
	}

	public function testOpenAiCompatibleSendsBearerAuthenticationButNotToTheStructuringEndpoint(): void {
		$calls = [];
		$transport = static function (string $method, string $url, ?array $payload, array $headers) use (&$calls): array {
			$calls[] = ['url' => $url, 'headers' => $headers];
			if (str_contains($url, '/api/chat')) {
				return ['message' => ['content' => json_encode(['summary' => 'S', 'event_title' => 'T',
					'event_date' => '2026-01-01'], JSON_THROW_ON_ERROR)]];
			}
			return ['choices' => [['message' => ['content' => '**Summary:** prose, not JSON.'], 'finish_reason' => 'stop']]];
		};
		$provider = new TopicDigestOpenAICompatible('https://api.groq.com/openai/v1', 'sk-secret', 10, $transport,
			structuringUrl: 'http://ollama-local', structuringModel: 'structuring-model');
		$provider->summarise('model', 'Title', 'Substantial article text.', 1_700_000_000);

		self::assertCount(2, $calls);
		self::assertStringContainsString('/chat/completions', $calls[0]['url']);
		self::assertSame('Bearer sk-secret', $calls[0]['headers']['Authorization'] ?? null);
		self::assertStringContainsString('/api/chat', $calls[1]['url']);
		self::assertArrayNotHasKey('Authorization', $calls[1]['headers']);
	}

	public function testOpenAiCompatibleBatchesTopicDecisionsInOneRequest(): void {
		$calls = 0;
		$transport = static function (string $method, string $url, ?array $payload) use (&$calls): array {
			$calls++;
			return ['choices' => [['message' => ['content' => json_encode(['decisions' => [
				['topic_id' => 1, 'matches' => true, 'confidence' => 0.9, 'reason' => 'It reports a release.',
					'event_title' => 'Model released'],
				['topic_id' => 2, 'matches' => false, 'confidence' => 0.1, 'reason' => 'Unrelated.', 'event_title' => ''],
			]], JSON_THROW_ON_ERROR)], 'finish_reason' => 'stop']]];
		};
		$topics = [
			['id' => 1, 'name' => 'AI releases', 'description' => 'New model releases', 'exclusions' => []],
			['id' => 2, 'name' => 'Physics', 'description' => 'New experimental results', 'exclusions' => []],
		];
		$provider = new TopicDigestOpenAICompatible('https://openrouter.ai/api/v1', 'sk-test', 10, $transport);
		$result = $provider->matchTopics('model', ['summary' => 'A model was released.', 'event_title' => 'Model released'], $topics);
		self::assertSame(1, $calls);
		self::assertTrue($result[1]['matches']);
		self::assertFalse($result[2]['matches']);
	}

	public function testOpenAiCompatibleBatchesEventDecisions(): void {
		$transport = static fn(string $method, string $url, ?array $payload): array => [
			'choices' => [['message' => ['content' => json_encode(['decisions' => [
				['candidate_id' => 'e:1', 'same_event' => true, 'confidence' => 0.9, 'reason' => 'Same release.'],
			]], JSON_THROW_ON_ERROR)], 'finish_reason' => 'stop']],
		];
		$provider = new TopicDigestOpenAICompatible('https://openrouter.ai/api/v1', 'sk-test', 10, $transport);
		$result = $provider->sameEvents('model', ['summary' => 'Release'], [
			['candidate_id' => 'e:1', 'title' => 'Release', 'occurred_at' => 1_700_000_000, 'explanation' => 'Release'],
		]);
		self::assertTrue($result['e:1']['same_event']);
	}

	public function testOpenAiCompatibleReshapesAFlatBooleanMapIntoTopicDecisions(): void {
		$transport = static fn(string $method, string $url, ?array $payload): array => [
			'choices' => [['message' => ['content' => json_encode(['1' => true, '2' => 'no'], JSON_THROW_ON_ERROR)],
				'finish_reason' => 'stop']],
		];
		$topics = [
			['id' => 1, 'name' => 'AI releases', 'description' => 'New model releases', 'exclusions' => []],
			['id' => 2, 'name' => 'Physics', 'description' => 'New experimental results', 'exclusions' => []],
		];
		$provider = new TopicDigestOpenAICompatible('https://openrouter.ai/api/v1', 'sk-test', 10, $transport);
		$result = $provider->matchTopics('model', ['summary' => 'A model was released.', 'event_title' => 'Model released'], $topics);
		self::assertTrue($result[1]['matches']);
		self::assertFalse($result[2]['matches']);
	}

	public function testOpenAiCompatibleRecoversJsonFromProseViaLocalStructuringFallback(): void {
		$calls = 0;
		$transport = static function (string $method, string $url, ?array $payload) use (&$calls): array {
			$calls++;
			if ($calls === 1) {
				return ['choices' => [['message' => ['content' => '**Event Title:** Something happened '
					. '**Summary:** Prose, not JSON.'], 'finish_reason' => 'stop']]];
			}
			self::assertSame('structuring-model', $payload['model'] ?? null);
			return ['message' => ['content' => json_encode(['summary' => 'Prose, not JSON.',
				'event_title' => 'Something happened', 'event_date' => '2026-01-01'], JSON_THROW_ON_ERROR)]];
		};
		$provider = new TopicDigestOpenAICompatible('https://openrouter.ai/api/v1', 'sk-test', 10, $transport,
			structuringUrl: 'http://ollama-local', structuringModel: 'structuring-model');
		$result = $provider->summarise('model', 'Title', 'Substantial article text.', 1_700_000_000);
		self::assertSame(2, $calls);
		self::assertSame('Something happened', $result['event_title']);
	}

	public function testOpenAiCompatibleThrowsWhenProseAndNoStructuringFallbackConfigured(): void {
		$transport = static fn(string $method, string $url, ?array $payload): array => [
			'choices' => [['message' => ['content' => 'Prose, not JSON.'], 'finish_reason' => 'stop']],
		];
		$provider = new TopicDigestOpenAICompatible('https://openrouter.ai/api/v1', 'sk-test', 10, $transport);
		$this->expectException(RuntimeException::class);
		$provider->summarise('model', 'Title', 'Substantial article text.', 1_700_000_000);
	}

	public function testOpenAiCompatibleStillRejectsAReplyMissingARequiredField(): void {
		$transport = static fn(string $method, string $url, ?array $payload): array => [
			'choices' => [['message' => ['content' => json_encode(['summary' => 'Summary', 'event_title' => 'Title'],
				JSON_THROW_ON_ERROR)], 'finish_reason' => 'stop']],
		];
		$provider = new TopicDigestOpenAICompatible('https://openrouter.ai/api/v1', 'sk-test', 10, $transport);
		$this->expectException(RuntimeException::class);
		$provider->summarise('model', 'Title', 'Substantial article text.', 1_700_000_000);
	}

	public function testOpenAiCompatibleDropsAdditionalStructuredFieldsInsteadOfLosingTheAnswer(): void {
		$transport = static fn(string $method, string $url, ?array $payload): array => [
			'choices' => [['message' => ['content' => json_encode(['summary' => 'Summary', 'event_title' => 'Title',
				'event_date' => '2026-01-01', 'extra' => 'ignored'], JSON_THROW_ON_ERROR)], 'finish_reason' => 'stop']],
		];
		$provider = new TopicDigestOpenAICompatible('https://openrouter.ai/api/v1', 'sk-test', 10, $transport);
		self::assertSame(['summary' => 'Summary', 'event_title' => 'Title', 'event_date' => '2026-01-01'],
			$provider->summarise('model', 'Title', 'Substantial article text.', 1_700_000_000));
	}

	public function testOpenAiCompatibleRetriesWithoutResponseFormatWhenTheEndpointRejectsIt(): void {
		$calls = 0;
		$transport = static function (string $method, string $url, ?array $payload) use (&$calls): array {
			$calls++;
			if ($calls === 1) {
				self::assertArrayHasKey('response_format', $payload);
				throw new RuntimeException('OpenAI-compatible request failed: POST ' . $url
					. ' (HTTP 400; response: response_format.json_schema is not supported by this model).');
			}
			self::assertArrayNotHasKey('response_format', $payload);
			self::assertArrayNotHasKey('reasoning_effort', $payload);
			return ['choices' => [['message' => ['content' => json_encode(['summary' => 'S', 'event_title' => 'T',
				'event_date' => '2026-01-01'], JSON_THROW_ON_ERROR)], 'finish_reason' => 'stop']]];
		};
		$provider = new TopicDigestOpenAICompatible('https://api.groq.com/openai/v1', 'sk-test', 10, $transport);
		$result = $provider->summarise('model', 'Title', 'Substantial article text.', 1_700_000_000);
		self::assertSame(2, $calls);
		self::assertSame('S', $result['summary']);
	}

	public function testOpenAiCompatibleCapsReasoningEffortAndRetriesWithABiggerBudgetIfStillEmpty(): void {
		$calls = 0;
		$transport = static function (string $method, string $url, ?array $payload) use (&$calls): array {
			$calls++;
			if ($calls === 1) {
				self::assertSame('low', $payload['reasoning_effort'] ?? null);
				self::assertSame(1600, $payload['max_tokens'] ?? null);
				return ['choices' => [['message' => ['role' => 'assistant', 'content' => null], 'finish_reason' => 'length']]];
			}
			self::assertSame(12800, $payload['max_tokens'] ?? null);
			return ['choices' => [['message' => ['content' => json_encode(['summary' => 'Recovered summary.',
				'event_title' => 'Recovered', 'event_date' => '2026-01-01'], JSON_THROW_ON_ERROR)],
				'finish_reason' => 'stop']]];
		};
		$provider = new TopicDigestOpenAICompatible('https://openrouter.ai/api/v1', 'sk-test', 10, $transport);
		$result = $provider->summarise('gpt-5-nano', 'Title', 'Substantial article text.', 1_700_000_000);
		self::assertSame(2, $calls);
		self::assertSame('Recovered summary.', $result['summary']);
	}

	public function testOpenAiCompatibleApiKeyNeverAppearsInARealRequestFailureMessage(): void {
		// Deliberately not using the transport closure here: it bypasses the real curl request-building code
		// entirely, which is exactly the code that must never interpolate the key into an exception message.
		// A closed local port fails instantly with no network access required.
		$provider = new TopicDigestOpenAICompatible('http://127.0.0.1:1', 'sk-super-secret-value', 1);
		try {
			$provider->test(['some-model']);
			self::fail('A connection to a closed port must fail.');
		} catch (Throwable $e) {
			self::assertStringNotContainsString('sk-super-secret-value', $e->getMessage());
			self::assertStringNotContainsString('Bearer', $e->getMessage());
		}
	}

	// -- TopicDigestTextProviderChain ---------------------------------------------------------------------------

	public function testChainUsesThePrimaryProviderWhenNotInCooldown(): void {
		$primary = new TopicDigestTestFakeTextProvider('primary summary', 'primary event');
		$fallback = new TopicDigestTestFakeTextProvider('fallback summary', 'fallback event');
		$chain = new TopicDigestTextProviderChain($primary, 'primary-summary-model', 'primary-judge-model', 'ollama',
			$fallback, 'fallback-summary-model', 'fallback-judge-model', 'openai-compatible', primaryInCooldown: false);

		self::assertFalse($chain->usesFallback());
		self::assertSame($primary, $chain->provider());
		self::assertSame('primary-summary-model', $chain->summaryModel());
		self::assertSame('ollama|primary-judge-model', $chain->judgeModelIdentity());
		self::assertSame('ollama', $chain->providerType());
	}

	public function testChainSwitchesToTheFallbackProviderWhenThePrimaryIsInCooldown(): void {
		$primary = new TopicDigestTestFakeTextProvider('primary summary', 'primary event');
		$fallback = new TopicDigestTestFakeTextProvider('fallback summary', 'fallback event');
		$chain = new TopicDigestTextProviderChain($primary, 'primary-summary-model', 'primary-judge-model', 'ollama',
			$fallback, 'fallback-summary-model', 'fallback-judge-model', 'openai-compatible', primaryInCooldown: true);

		self::assertTrue($chain->usesFallback());
		self::assertSame($fallback, $chain->provider());
		self::assertSame('fallback-summary-model', $chain->summaryModel());
		self::assertSame('openai-compatible|fallback-judge-model', $chain->judgeModelIdentity());
		// This is the switch TopicDigestProcessor::runActive() checks to decide whether worker concurrency may
		// actually dispatch several articles at once: 'ollama' as the primary must never enable it, but a
		// fallback that switches the *effective* provider to 'openai-compatible' should.
		self::assertSame('openai-compatible', $chain->providerType());
	}

	public function testChainStaysOnThePrimaryWhenNoFallbackIsConfiguredEvenDuringCooldown(): void {
		$primary = new TopicDigestTestFakeTextProvider('primary summary', 'primary event');
		$chain = new TopicDigestTextProviderChain($primary, 'primary-summary-model', 'primary-judge-model', 'ollama',
			null, null, null, null, primaryInCooldown: true);

		self::assertFalse($chain->hasFallback());
		self::assertFalse($chain->usesFallback());
		self::assertSame($primary, $chain->provider());
	}

	public function testChainRejectsAFallbackProviderWithoutItsModelsOrLabel(): void {
		$primary = new TopicDigestTestFakeTextProvider('s', 'e');
		$fallback = new TopicDigestTestFakeTextProvider('s', 'e');
		$this->expectException(InvalidArgumentException::class);
		new TopicDigestTextProviderChain($primary, 'primary-summary-model', 'primary-judge-model', 'ollama',
			$fallback, null, 'fallback-judge-model', 'openai-compatible', primaryInCooldown: false);
	}

	// -- Generalized fallback tracking in TopicDigestStore -------------------------------------------------------

	public function testPrimaryTextFallbackTrackingIsIndependentOfTheLegacyCloudTracking(): void {
		self::assertSame(0, $this->store->primaryTextFallbackUntil());
		$this->store->markPrimaryTextFallback(1_700_001_000);
		self::assertSame(1_700_001_000, $this->store->primaryTextFallbackUntil());
		// The legacy Ollama Cloud->local mechanism is untouched by the generalized one.
		self::assertSame(0, $this->store->cloudUnavailableUntil());
	}

	public function testEmbeddingIdentityTrackingIsSeparateFromLegacyProfileTracking(): void {
		self::assertNull($this->store->lastEmbeddingIdentity());
		$this->store->setLastEmbeddingIdentity('ollama|http://ollama:11434|qwen3-embedding:0.6b');
		self::assertSame('ollama|http://ollama:11434|qwen3-embedding:0.6b', $this->store->lastEmbeddingIdentity());
		// Unrelated to the (still-present, still-tested) legacy profile tracker.
		self::assertNull($this->store->lastOllamaProfile());
	}

	// -- TopicDigestConcurrentDispatcher (worker concurrency) -----------------------------------------------------

	public function testDispatcherStartsEveryOperationBeforeAnyResumes(): void {
		$dispatcher = new TopicDigestConcurrentDispatcher(5, 5);
		$order = [];
		$dispatcher->run([
			function () use (&$order): void {
				$order[] = 'a-start';
				Fiber::suspend();
				$order[] = 'a-resume';
			},
			function () use (&$order): void {
				$order[] = 'b-start';
				Fiber::suspend();
				$order[] = 'b-resume';
			},
		]);
		// If run() ran the first operation to completion before even starting the second (i.e. was secretly
		// sequential), 'b-start' could never appear before 'a-resume'.
		self::assertSame(['a-start', 'b-start'], array_slice($order, 0, 2));
	}

	public function testDispatcherIsolatesOneOperationsFailureFromTheOthers(): void {
		$dispatcher = new TopicDigestConcurrentDispatcher(5, 5);
		$results = $dispatcher->run([
			fn(): string => 'ok-1',
			fn(): string => throw new RuntimeException('task 2 failed'),
			fn(): string => 'ok-3',
		]);
		self::assertSame(['value' => 'ok-1', 'error' => null], $results[0]);
		self::assertNull($results[1]['value']);
		self::assertInstanceOf(RuntimeException::class, $results[1]['error']);
		self::assertSame('task 2 failed', $results[1]['error']->getMessage());
		self::assertSame(['value' => 'ok-3', 'error' => null], $results[2]);
	}

	public function testDispatcherClassifiesQuotaStatusCodesFromEitherTransport(): void {
		$dispatcher = new TopicDigestConcurrentDispatcher(5, 5);
		$server = @stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
		if ($server === false) {
			self::markTestSkipped("Cannot bind a loopback socket: {$errstr}");
		}
		$name = stream_socket_get_name($server, false);
		$port = (int)substr($name, strrpos($name, ':') + 1);
		$respond = function () use ($server): void {
			$conn = stream_socket_accept($server, 5.0);
			self::assertNotFalse($conn);
			stream_set_blocking($conn, true);
			while (!str_contains((string)fread($conn, 8192), "\r\n\r\n")) {
			}
			$body = '{"error":"rate limited"}';
			fwrite($conn, "HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\nContent-Length: "
				. strlen($body) . "\r\nConnection: close\r\n\r\n" . $body);
			fclose($conn);
			fclose($server);
		};
		$transport = $dispatcher->openAiTransport();
		$results = $dispatcher->run([
			function () use ($transport, $port): array {
				return $transport('POST', "http://127.0.0.1:{$port}/chat/completions", ['model' => 'x'], []);
			},
			function () use ($respond): void {
				$respond();
			},
		]);
		self::assertInstanceOf(OllamaQuotaExceededException::class, $results[0]['error']);
	}

	public function testDispatcherRunsTwoRealRequestsConcurrentlyNotSerially(): void {
		if (!extension_loaded('pcntl') || !extension_loaded('posix')) {
			self::markTestSkipped('pcntl/posix are required to prove genuine overlap with a forking loopback server.');
		}
		$serverScript = sys_get_temp_dir() . '/topic-digest-loopback-' . bin2hex(random_bytes(8)) . '.php';
		// Forks a child per accepted connection so both requests are served in parallel: a server that instead
		// handled them one at a time would itself be the serial bottleneck, since a TCP connection completing
		// does not require the server to have called accept() yet, making a non-forking server unable to prove
		// anything about the *dispatcher's* concurrency.
		file_put_contents($serverScript, <<<'PHP'
<?php
$port = (int)$argv[1];
$server = stream_socket_server("tcp://127.0.0.1:{$port}", $errno, $errstr);
if ($server === false) {
	exit(1);
}
$children = [];
for ($i = 0; $i < 2; $i++) {
	$conn = stream_socket_accept($server, 10.0);
	if ($conn === false) {
		exit(1);
	}
	$pid = pcntl_fork();
	if ($pid === 0) {
		stream_set_blocking($conn, true);
		while (!str_contains((string)fread($conn, 8192), "\r\n\r\n")) {
		}
		usleep(400_000);
		$body = '{"choices":[{"message":{"content":"ok"}}]}';
		fwrite($conn, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
			. strlen($body) . "\r\nConnection: close\r\n\r\n" . $body);
		fclose($conn);
		exit(0);
	}
	$children[] = $pid;
	fclose($conn);
}
foreach ($children as $pid) {
	pcntl_waitpid($pid, $status);
}
PHP);
		$port = random_int(20000, 60000);
		$process = proc_open([PHP_BINARY, $serverScript, (string)$port], [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
		self::assertNotFalse($process);
		usleep(200_000);
		try {
			$dispatcher = new TopicDigestConcurrentDispatcher(5, 8);
			$transportA = $dispatcher->openAiTransport();
			$transportB = $dispatcher->openAiTransport();
			$startedAt = microtime(true);
			$results = $dispatcher->run([
				fn(): array => $transportA('POST', "http://127.0.0.1:{$port}/chat/completions", ['model' => 'x'], []),
				fn(): array => $transportB('POST', "http://127.0.0.1:{$port}/chat/completions", ['model' => 'x'], []),
			]);
			$elapsed = microtime(true) - $startedAt;
		} finally {
			proc_terminate($process);
			proc_close($process);
			unlink($serverScript);
		}
		self::assertNull($results[0]['error']);
		self::assertNull($results[1]['error']);
		self::assertSame('ok', $results[0]['value']['choices'][0]['message']['content']);
		// Two responses each deliberately delayed ~0.4s: a dispatcher that only ever has one curl handle in
		// flight at a time (i.e. is secretly serial despite the API looking concurrent) takes >=0.8s; a
		// genuinely concurrent one takes roughly one delay's worth of wall time. Generous margin to avoid
		// flakiness on a loaded CI runner.
		self::assertLessThan(0.75, $elapsed);
	}

	// -- Queue claiming under concurrency --------------------------------------------------------------------

	public function testClaimCalledSeriallyYieldsDistinctNonOverlappingJobs(): void {
		$ids = [];
		for ($i = 0; $i < 4; $i++) {
			$entry = new FreshRSS_Entry(4, 'guid-' . $i, 'Article ' . $i, '', 'Body',
				'https://example.com/' . $i, 1_700_000_000 + $i);
			$entry->_id('170000000000000' . $i);
			$this->store->enqueue($entry, 2, 'pipeline', 100 - $i);
			$ids[] = $entry->id();
		}
		$claimed = [];
		for ($i = 0; $i < 4; $i++) {
			$job = $this->store->claim(600);
			self::assertNotNull($job);
			$claimed[] = $job['entry_id'];
		}
		// Claimed in priority order (highest first), matching claim()'s own ordering — and, crucially,
		// distinct: no entry_id repeats, so calling claim() serially to gather a wavefront never double-claims.
		self::assertSame($ids, $claimed);
		self::assertSame(4, count(array_unique($claimed)));
		self::assertNull($this->store->claim(600), 'A fifth call finds nothing left to claim.');
	}

	/** @return array<string,mixed> */
	private function job(string $entryId, string $title): array {
		return [
			'entry_id' => $entryId, 'feed_id' => 1, 'category_id' => 1, 'title' => $title,
			'author' => '', 'link' => 'https://example.com/' . $entryId, 'published_at' => 1_700_000_000,
			'content_hash' => hash('md5', $title), 'pipeline_hash' => 'pipeline', 'rss_text' => 'Article',
		];
	}
}

/** A minimal TopicDigestTextProvider double for TopicDigestTextProviderChain tests: no network, no FreshRSS. */
final class TopicDigestTestFakeTextProvider implements TopicDigestTextProvider {
	public function __construct(private readonly string $summary, private readonly string $eventTitle) {
	}

	public function test(array $models): void {
	}

	public function summarise(string $model, string $title, string $text, int $publishedAt): array {
		return ['summary' => $this->summary, 'event_title' => $this->eventTitle, 'event_date' => '2026-01-01'];
	}

	public function matchTopic(string $model, array $summary, array $topic): array {
		return ['matches' => true, 'confidence' => 1.0, 'reason' => 'fake', 'event_title' => $this->eventTitle];
	}

	public function matchTopics(string $model, array $summary, array $topics): array {
		$result = [];
		foreach ($topics as $topic) {
			$result[(int)$topic['id']] = $this->matchTopic($model, $summary, $topic);
		}
		return $result;
	}

	public function sameEvent(string $model, array $summary, array $event): array {
		return ['same_event' => true, 'confidence' => 1.0, 'reason' => 'fake'];
	}

	public function sameEvents(string $model, array $summary, array $events): array {
		$result = [];
		foreach ($events as $event) {
			$result[(string)$event['candidate_id']] = $this->sameEvent($model, $summary, $event);
		}
		return $result;
	}
}
