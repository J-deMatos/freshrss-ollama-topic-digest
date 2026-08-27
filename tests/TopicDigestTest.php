<?php
declare(strict_types=1);

use PHPUnit\Framework\TestCase;

require_once dirname(__DIR__) . '/TopicDigestStore.php';
require_once dirname(__DIR__) . '/ArticleSummaryCache.php';
require_once dirname(__DIR__) . '/TopicDigestOllama.php';
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

	public function testQueueIsIdempotentAndPipelineChangesRequeue(): void {
		$entry = new FreshRSS_Entry(4, 'guid', 'Model released', '', 'Detailed release announcement.',
			'https://example.com/model', 1_700_000_000);
		$entry->_id('1700000000000000');
		self::assertTrue($this->store->enqueue($entry, 2, 'pipeline-one'));
		self::assertFalse($this->store->enqueue($entry, 2, 'pipeline-one'));
		self::assertTrue($this->store->enqueue($entry, 2, 'pipeline-two'));
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

	public function testEventsMoveToTopWhenTheyReceiveTheLatestSource(): void {
		$topicId = $this->store->saveTopic(['name' => 'AI', 'description' => 'AI model releases',
			'enabled' => true, 'all_feeds' => true]);
		$first = $this->store->addMatch($topicId, $this->job('100', 'First event'), 'Feed', 'First event',
			1_700_000_000, 'First event', [1.0], null);
		$this->store->addMatch($topicId, $this->job('200', 'Second event'), 'Feed', 'Second event',
			1_700_000_100, 'Second event', [1.0], null);
		$coverage = $this->job('300', 'New coverage of first');
		$coverage['published_at'] = 1_700_000_200;
		$this->store->addMatch($topicId, $coverage, 'Feed', 'First event',
			1_700_000_200, 'New coverage', [1.0], $first['event_id']);

		$events = $this->store->events($topicId);
		self::assertSame($first['event_id'], (int)$events[0]['id']);
		self::assertSame('300', $events[0]['sources'][0]['entry_id']);
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
		self::assertSame($revision + 1, $this->store->pipelineRevision());
		self::assertTrue($this->store->backfill()['active']);

		$this->store->recordRebuildUnread('100');
		self::assertTrue($this->store->isRebuildUnread('100'));
		self::assertFalse($this->store->isProtected('100'));
		$this->store->completeRebuildRestore('100');
		self::assertFalse($this->store->isPendingRebuildRestore('100'));
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

	public function testOllamaRejectsAdditionalStructuredFields(): void {
		$transport = static fn(string $method, string $path, ?array $payload): array => [
			'message' => ['content' => json_encode([
				'summary' => 'Summary', 'event_title' => 'Title', 'event_date' => '2026-01-01', 'extra' => 'bad',
			], JSON_THROW_ON_ERROR)],
		];
		$ollama = new TopicDigestOllama('http://ollama', 10, $transport);
		$this->expectException(RuntimeException::class);
		$ollama->summarise('model', 'Title', 'Article', 1_700_000_000);
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

	/** @return array<string,mixed> */
	private function job(string $entryId, string $title): array {
		return [
			'entry_id' => $entryId, 'feed_id' => 1, 'category_id' => 1, 'title' => $title,
			'author' => '', 'link' => 'https://example.com/' . $entryId, 'published_at' => 1_700_000_000,
			'content_hash' => hash('md5', $title), 'pipeline_hash' => 'pipeline', 'rss_text' => 'Article',
		];
	}
}
