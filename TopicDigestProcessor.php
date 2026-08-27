<?php
declare(strict_types=1);

/** @phpstan-import-type TopicDigestConfig from TopicDigestExtension */
final class TopicDigestProcessor {
	private const TOPIC_DECISION_REVISION = 'topic-batch-v2';
	private const EVENT_DECISION_REVISION = 'event-batch-v1';
	private TopicDigestStore $store;
	/** @var TopicDigestConfig */
	private array $config;
	private TopicDigestOllama $ollama;

	public function __construct(private readonly TopicDigestExtension $extension) {
		$this->store = $extension->store();
		$this->config = $extension->configuration();
		$this->ollama = new TopicDigestOllama((string)$this->config['ollama_url'], (int)$this->config['timeout']);
	}

	/** @return array{processed:int,failed:int,backfill_scanned:int} */
	public function run(int $limit): array {
		if ($this->store->isPaused()) {
			return ['processed' => 0, 'failed' => 0, 'backfill_scanned' => 0];
		}
		return $this->runActive($limit);
	}

	/** @return array{processed:int,failed:int,backfill_scanned:int} */
	private function runActive(int $limit): array {
		$processed = 0;
		$failed = 0;
		$scanned = 0;
		while (!$this->store->isPaused() && $this->store->backfill()['active']) {
			$count = $this->extension->enqueueBackfillPage(1000);
			$scanned += $count;
			if ($count === 0 && $this->store->backfill()['active']) {
				throw new RuntimeException('Topic Digest archive scan did not advance.');
			}
		}
		if ($this->store->isPaused()) {
			return ['processed' => 0, 'failed' => 0, 'backfill_scanned' => $scanned];
		}
		for ($index = 0; $index < $limit; $index++) {
			$job = $this->store->claim(max(600, (int)$this->config['timeout'] * 10));
			if ($job === null) {
				break;
			}
			$jobStartedAt = hrtime(true);
			try {
				if ($this->process($job)) {
					$processed++;
					try {
						if (!$this->store->recordProcessingActivity((string)$job['entry_id'],
								(hrtime(true) - $jobStartedAt) / 1_000_000_000)) {
							Minz_Log::error('Topic Digest could not attach processing metrics to the completed job.');
						}
					} catch (Throwable $e) {
						Minz_Log::error('Topic Digest processing metrics error: ' . $e->getMessage());
					}
				}
			} catch (Throwable $e) {
				if ($this->store->failCurrent($job, $e->getMessage())) {
					$failed++;
					Minz_Log::error('Topic Digest worker error: ' . $e->getMessage());
				}
			}
		}
		return ['processed' => $processed, 'failed' => $failed, 'backfill_scanned' => $scanned];
	}

	/** @param array<string,mixed> $topic @return list<array{title:string,matches:bool,confidence:float,reason:string}> */
	public function previewTopic(array $topic, int $limit = 10): array {
		$results = [];
		$entries = FreshRSS_Factory::createEntryDao()->listWhere(sort: 'id', order: 'DESC', limit: max(1, min(20, $limit)));
		foreach ($entries as $entry) {
			if ($this->extension->isSyntheticFeed($entry->feedId())) {
				continue;
			}
			$text = TopicDigestScraper::rssText($entry->content(false));
			if ((bool)$this->config['scraping'] && TopicDigestScraper::isInsufficient($entry->content(false))) {
				$text = TopicDigestScraper::fetch($entry->link(raw: true), min(60, (int)$this->config['timeout'])) ?? $text;
			}
			$summary = $this->ollama->summarise((string)$this->config['summary_model'],
				htmlspecialchars_decode($entry->title(), ENT_QUOTES), $text, $entry->date(true));
			$decision = $this->ollama->matchTopic((string)$this->config['judge_model'], $summary, $topic);
			$results[] = ['title' => htmlspecialchars_decode($entry->title(), ENT_QUOTES),
				'matches' => $decision['matches'] && $decision['confidence'] >= (float)$topic['confidence'],
				'confidence' => $decision['confidence'], 'reason' => $decision['reason']];
		}
		return $results;
	}

	/** @param array<string,mixed> $job */
	private function process(array $job): bool {
		$entry = FreshRSS_Factory::createEntryDao()->searchById((string)$job['entry_id']);
		if ($entry === null) {
			$this->store->completeRebuildRestore((string)$job['entry_id']);
			return $this->store->completeCurrent($job, 'skipped', 'Entry no longer exists.');
		}
		if ($this->extension->isSyntheticFeed($entry->feedId())) {
			return $this->store->completeCurrent($job, 'skipped', 'Synthetic digest entry.');
		}
		if (!hash_equals((string)$job['content_hash'], $entry->hash())
				|| !hash_equals((string)$job['pipeline_hash'], $this->extension->pipelineHash())) {
			$feed = $entry->feed();
			$this->store->enqueue($entry, $feed?->categoryId() ?? 0, $this->extension->pipelineHash(),
				archive: (bool)($job['is_archive'] ?? false));
			return false;
		}
		foreach ($this->store->detachChangedSources($entry->id(), $entry->hash()) as $topicId) {
			if ($this->store->topic($topicId) !== null) {
				$this->extension->synchroniseTopic($topicId, false);
			}
		}
		foreach ($this->store->topicIdsForSource($entry->id()) as $topicId) {
			$membershipTopic = $this->store->topic($topicId);
			if ($membershipTopic === null || ($membershipTopic['enabled'] && !$this->topicAccepts($membershipTopic, $job))) {
				if ($this->store->removeSourceMembership($topicId, $entry->id()) && $membershipTopic !== null) {
					$this->extension->synchroniseTopic($topicId, false);
				}
			}
		}
		if ($entry->isFavorite() || $this->store->isProtected($entry->id())) {
			$this->extension->finishRebuildForEntry($entry, false);
			return $this->store->completeCurrent($job, 'skipped', 'Article is protected by the user.');
		}
		if (!$this->hasEligibleTopic($job)) {
			$this->extension->finishRebuildForEntry($entry, false);
			return $this->store->completeCurrent($job, 'skipped', 'No active topic includes this article.');
		}

		$stored = $this->store->summary($entry->id());
		if ($stored === null || !hash_equals((string)$stored['content_hash'], $entry->hash())
				|| !hash_equals((string)($stored['analysis_hash'] ?? ''), $this->extension->analysisHash())) {
			$shared = $this->sharedSummary($job);
			if ($shared !== null) {
				$text = $shared['source_text'] !== ''
					? $shared['source_text'] : TopicDigestScraper::rssText((string)$job['rss_text']);
				$summary = [
					'summary' => $shared['summary_text'],
					'event_title' => trim($shared['event_title']) !== ''
						? $shared['event_title'] : (string)$job['title'],
					'event_date' => trim($shared['event_date']) !== ''
						? $shared['event_date'] : date('Y-m-d', (int)$job['published_at']),
				];
				$embedding = $shared['embedding'];
			} else {
				$text = TopicDigestScraper::rssText((string)$job['rss_text']);
				if ((bool)$this->config['scraping'] && TopicDigestScraper::isInsufficient((string)$job['rss_text'])) {
					$text = TopicDigestScraper::fetch((string)$job['link'], min(60, (int)$this->config['timeout'])) ?? $text;
				}
				$summary = $this->ollama->summarise((string)$this->config['summary_model'],
					(string)$job['title'], $text, (int)$job['published_at']);
				$embedding = $this->ollama->embed((string)$this->config['embedding_model'], $this->summaryText($summary));
			}
			$feedName = htmlspecialchars_decode($entry->feed()?->name(raw: true) ?? '', ENT_QUOTES);
			if (!$this->store->saveSummaryIfCurrent($job, $this->extension->analysisHash(), $feedName,
					json_encode($summary, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR), $text, $embedding)) {
				return false;
			}
			if ($shared === null) {
				$this->publishSharedSummary($job, $summary, $text, $embedding);
			}
			$stored = $this->store->summary($entry->id());
		}
		if ($stored === null) {
			throw new RuntimeException('Could not persist the article summary.');
		}
		$summary = json_decode((string)$stored['summary'], true, flags: JSON_THROW_ON_ERROR);
		if (!is_array($summary)) {
			throw new RuntimeException('Stored Topic Digest summary is invalid.');
		}
		$embedding = $this->decodeEmbedding((string)$stored['embedding']);
		$matched = false;
		$candidateTopics = array_values(array_filter($this->candidateTopics($job, $embedding),
			fn(array $topic): bool => !$this->store->isRejected((int)$topic['id'], $entry->id())));
		$topicDecisions = $this->topicDecisions($job, $summary, $candidateTopics);
		if ($topicDecisions === null) {
			return false;
		}
		foreach ($candidateTopics as $topic) {
			if ($this->store->isRejected((int)$topic['id'], $entry->id())) {
				continue;
			}
			$decision = $topicDecisions[(int)$topic['id']];
			if (!$this->store->isCurrentJob($job) || $this->store->topic((int)$topic['id']) === null) {
				return false;
			}
			if (!$decision['matches'] || $decision['confidence'] < (float)$topic['confidence']) {
				if ($this->store->removeSourceMembership((int)$topic['id'], $entry->id())) {
					$this->extension->synchroniseTopic((int)$topic['id'], false);
				}
				continue;
			}
			$eventTitle = $decision['event_title'] !== '' ? $decision['event_title'] : (string)$summary['event_title'];
			$occurredAt = strtotime((string)($summary['event_date'] ?? '')) ?: (int)$job['published_at'];
			if ($occurredAt < 1 || $occurredAt > time() + 86400) {
				$occurredAt = (int)$job['published_at'];
			}
			$fingerprint = hash('sha256', mb_strtolower(trim($eventTitle), 'UTF-8'));
			if ($this->store->isRejected((int)$topic['id'], $entry->id(), $fingerprint)) {
				continue;
			}
			$eventResolution = $this->eventResolution((int)$topic['id'], $job, $summary, $embedding);
			if ($eventResolution === null) {
				return false;
			}
			if ($eventResolution['rejected']) {
				continue;
			}
			$eventId = $eventResolution['event_id'];
			if (!$this->store->isCurrentJob($job)) {
				return false;
			}
			try {
				$result = $this->store->addMatch((int)$topic['id'], $job, (string)$stored['feed_name'], $eventTitle,
					$occurredAt, $decision['reason'], $embedding, $eventId);
			} catch (DomainException) {
				continue;
			}
			if ($result === null) {
				return false;
			}
			$this->extension->synchroniseTopic((int)$topic['id'], $result['new_event']);
			$matched = true;
		}
		if ($matched) {
			$entryDao = FreshRSS_Factory::createEntryDao();
			$current = $entryDao->searchById($entry->id());
			if ($current !== null && hash_equals($entry->hash(), $current->hash()) && !$current->isRead()
					&& !$current->isFavorite() && !$this->store->isProtected($current->id())
					&& ((method_exists($current, 'lastUserModified') && $current->lastUserModified() === null)
						|| (!method_exists($current, 'lastUserModified') && $this->extension->supportsEntriesReadHook()))) {
				$affected = $entryDao->markRead($current->id(), true);
				if ($affected === false) {
					throw new RuntimeException('Could not mark the matched source as read.');
				}
			}
		}
		$this->extension->finishRebuildForEntry($entry, $matched);
		return $this->store->completeCurrent($job);
	}

	/** @param array<string,mixed> $job @param list<float> $embedding @return list<array<string,mixed>> */
	private function candidateTopics(array $job, array $embedding): array {
		$candidates = [];
		$memberships = array_fill_keys($this->store->topicIdsForSource((string)$job['entry_id']), true);
		foreach ($this->store->topics(true) as $topic) {
			if (!$this->topicAccepts($topic, $job)) {
				continue;
			}
			$topicEmbedding = $this->topicEmbedding($topic);
			$candidates[] = ['score' => self::cosine($embedding, $topicEmbedding), 'topic' => $topic,
				'membership' => isset($memberships[(int)$topic['id']])];
		}
		usort($candidates, static fn(array $left, array $right): int => $right['score'] <=> $left['score']);
		$selected = array_slice($candidates, 0, 5);
		foreach ($candidates as $candidate) {
			if ($candidate['membership'] && !in_array($candidate, $selected, true)) {
				$selected[] = $candidate;
			}
		}
		return array_map(static fn(array $candidate): array => $candidate['topic'], $selected);
	}

	/**
	 * @param array<string,mixed> $job
	 * @param array<string,mixed> $summary
	 * @param list<array<string,mixed>> $topics
	 * @return array<int,array{matches:bool,confidence:float,reason:string,event_title:string}>|null
	 */
	private function topicDecisions(array $job, array $summary, array $topics): ?array {
		$judgeModel = (string)$this->config['judge_model'];
		$judgeRevision = $judgeModel . "\n" . self::TOPIC_DECISION_REVISION;
		$inputHash = $this->decisionInputHash($job, $summary);
		$decisions = [];
		$uncached = [];
		foreach ($topics as $topic) {
			$id = (int)$topic['id'];
			$cached = $this->store->topicDecision((string)$job['entry_id'], $inputHash, $id,
				(string)$topic['rule_hash'], $judgeRevision);
			if ($cached === null) {
				$uncached[] = $topic;
			} else {
				$decisions[$id] = $cached;
			}
		}
		foreach (array_chunk($uncached, 8) as $batch) {
			$batchDecisions = $this->ollama->matchTopics($judgeModel, $summary, $batch);
			if (!$this->store->isCurrentJob($job)) {
				return null;
			}
			foreach ($batch as $topic) {
				$id = (int)$topic['id'];
				$decision = $batchDecisions[$id];
				$this->store->saveTopicDecision((string)$job['entry_id'], $inputHash, $id,
					(string)$topic['rule_hash'], $judgeRevision, $decision);
				$decisions[$id] = $decision;
			}
		}
		return $decisions;
	}

	/** @param array<string,mixed> $topic @param array<string,mixed> $job */
	private function topicAccepts(array $topic, array $job): bool {
		$feedMatch = $topic['all_feeds'] || in_array((int)$job['feed_id'], $topic['feed_ids'], true);
		$categoryMatch = $topic['all_categories'] || in_array((int)$job['category_id'], $topic['category_ids'], true);
		if (!$feedMatch && !$categoryMatch) {
			return false;
		}
		return match ($topic['backfill_mode']) {
			'future' => !(bool)($job['is_archive'] ?? false),
			'days' => (int)$job['published_at'] >= time() - ((int)$topic['backfill_days'] * 86400),
			default => true,
		};
	}

	/** @param array<string,mixed> $job */
	private function hasEligibleTopic(array $job): bool {
		foreach ($this->store->topics(true) as $topic) {
			if ($this->topicAccepts($topic, $job)) {
				return true;
			}
		}
		return false;
	}

	/** @param array<string,mixed> $topic @return list<float> */
	private function topicEmbedding(array &$topic): array {
		$stored = $topic['description_embedding'] ?? null;
		if (is_string($stored) && $stored !== '') {
			return $this->decodeEmbedding($stored);
		}
		$text = (string)$topic['description'] . "\nExclusions: " . implode('; ', $topic['exclusions']);
		$embedding = $this->ollama->embed((string)$this->config['embedding_model'], $text);
		$this->store->saveTopicEmbedding((int)$topic['id'], (string)$topic['rule_hash'], $embedding);
		return $embedding;
	}

	/**
	 * @param array<string,mixed> $job
	 * @param array<string,mixed> $summary
	 * @param list<float> $embedding
	 * @return array{rejected:bool,event_id:?int}|null
	 */
	private function eventResolution(int $topicId, array $job, array $summary, array $embedding): ?array {
		$rejectedCandidates = [];
		foreach ($this->store->rejectedEventCandidates($topicId) as $event) {
			try {
				$score = self::cosine($embedding, $this->decodeEmbedding((string)$event['embedding']));
			} catch (Throwable) {
				$score = -1.0;
			}
			if ($score >= 0.70 || count($rejectedCandidates) < 5) {
				$rejectedCandidates[] = ['score' => $score, 'event' => $event];
			}
		}
		usort($rejectedCandidates, static fn(array $left, array $right): int => $right['score'] <=> $left['score']);
		$rejectedCandidates = array_slice($rejectedCandidates, 0, 5);

		$eventCandidates = [];
		$recent = $this->store->eventCandidates($topicId, time() - (3650 * 86400));
		foreach ($recent as $event) {
			try {
				$eventEmbedding = $this->decodeEmbedding((string)$event['embedding']);
			} catch (Throwable) {
				continue;
			}
			$score = self::cosine($embedding, $eventEmbedding);
			if ($score >= 0.70) {
				$eventCandidates[] = ['score' => $score, 'event' => $event];
			}
		}
		if ($eventCandidates === []) {
			$eventCandidates = array_map(static fn(array $event): array => ['score' => -1.0, 'event' => $event],
				array_slice($recent, 0, 5));
		}
		usort($eventCandidates, static fn(array $left, array $right): int => $right['score'] <=> $left['score']);
		$eventCandidates = array_slice($eventCandidates, 0, 5);

		$candidates = [];
		foreach ($rejectedCandidates as $candidate) {
			$event = $candidate['event'];
			$candidates[] = $this->decisionCandidate('r:' . (string)$event['fingerprint'], 'rejected', $event);
		}
		foreach ($eventCandidates as $candidate) {
			$event = $candidate['event'];
			$candidates[] = $this->decisionCandidate('e:' . (string)$event['id'], 'event', $event);
		}
		$decisions = $this->eventDecisions($topicId, $job, $summary, $candidates);
		if ($decisions === null) {
			return null;
		}
		foreach ($candidates as $candidate) {
			$decision = $decisions[$candidate['candidate_id']];
			if ($candidate['kind'] === 'rejected' && $decision['same_event'] && $decision['confidence'] >= 0.85) {
				return ['rejected' => true, 'event_id' => null];
			}
		}
		foreach ($candidates as $candidate) {
			$decision = $decisions[$candidate['candidate_id']];
			if ($candidate['kind'] === 'event' && $decision['same_event'] && $decision['confidence'] >= 0.85) {
				return ['rejected' => false, 'event_id' => (int)$candidate['event']['id']];
			}
		}
		return ['rejected' => false, 'event_id' => null];
	}

	/** @param array<string,mixed> $event @return array<string,mixed> */
	private function decisionCandidate(string $candidateId, string $kind, array $event): array {
		$hash = hash('sha256', json_encode([(string)$event['title'], (int)$event['occurred_at'],
			(string)$event['explanation']], JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR));
		return ['candidate_id' => $candidateId, 'candidate_hash' => $hash, 'kind' => $kind, 'event' => $event,
			'title' => (string)$event['title'], 'occurred_at' => (int)$event['occurred_at'],
			'explanation' => (string)$event['explanation']];
	}

	/**
	 * @param array<string,mixed> $job
	 * @param array<string,mixed> $summary
	 * @param list<array<string,mixed>> $candidates
	 * @return array<string,array{same_event:bool,confidence:float,reason:string}>|null
	 */
	private function eventDecisions(int $topicId, array $job, array $summary, array $candidates): ?array {
		$judgeModel = (string)$this->config['judge_model'];
		$judgeRevision = $judgeModel . "\n" . self::EVENT_DECISION_REVISION;
		$inputHash = $this->decisionInputHash($job, $summary);
		$decisions = [];
		$uncached = [];
		foreach ($candidates as $candidate) {
			$id = (string)$candidate['candidate_id'];
			$cached = $this->store->eventDecision((string)$job['entry_id'], $inputHash, $topicId,
				$id, (string)$candidate['candidate_hash'], $judgeRevision);
			if ($cached === null) {
				$uncached[] = $candidate;
			} else {
				$decisions[$id] = $cached;
			}
		}
		foreach (array_chunk($uncached, 10) as $batch) {
			$batchDecisions = $this->ollama->sameEvents($judgeModel, $summary, $batch);
			if (!$this->store->isCurrentJob($job)) {
				return null;
			}
			foreach ($batch as $candidate) {
				$id = (string)$candidate['candidate_id'];
				$decision = $batchDecisions[$id];
				$this->store->saveEventDecision((string)$job['entry_id'], $inputHash, $topicId,
					$id, (string)$candidate['candidate_hash'], $judgeRevision, $decision);
				$decisions[$id] = $decision;
			}
		}
		return $decisions;
	}

	/** @param array<string,mixed> $job @param array<string,mixed> $summary */
	private function decisionInputHash(array $job, array $summary): string {
		return hash('sha256', (string)$job['content_hash'] . "\n"
			. json_encode($summary, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR));
	}

	/** @return list<float> */
	private function decodeEmbedding(string $json): array {
		$values = json_decode($json, true, flags: JSON_THROW_ON_ERROR);
		if (!is_array($values) || $values === []) {
			throw new RuntimeException('Stored embedding is invalid.');
		}
		return array_map('floatval', $values);
	}

	/**
	 * @param array<string,mixed> $job
	 * @return array{summary_text:string,source_text:string,embedding:list<float>,event_title:string,event_date:string,
	 *     origin:string}|null
	 */
	private function sharedSummary(array $job): ?array {
		try {
			return $this->extension->sharedSummaryCache()->find(
				(string)$job['entry_id'], (string)$job['content_hash'], (string)$this->config['summary_model'],
				(string)$this->config['embedding_model']
			);
		} catch (Throwable $e) {
			Minz_Log::error('Topic Digest shared-summary read error: ' . $e->getMessage());
			return null;
		}
	}

	/** @param array<string,mixed> $job @param array<string,mixed> $summary @param list<float> $embedding */
	private function publishSharedSummary(array $job, array $summary, string $sourceText, array $embedding): void {
		try {
			$this->extension->sharedSummaryCache()->save(
				(string)$job['entry_id'], (string)$job['content_hash'], (string)$this->config['summary_model'],
				(string)$this->config['embedding_model'], $this->summaryText($summary), $sourceText, $embedding,
				(string)$summary['event_title'], (string)$summary['event_date'], 'topic_digest'
			);
		} catch (Throwable $e) {
			Minz_Log::error('Topic Digest shared-summary write error: ' . $e->getMessage());
		}
	}

	/** @param array<string,mixed> $summary */
	private function summaryText(array $summary): string {
		return (string)$summary['event_title'] . "\n" . (string)$summary['event_date'] . "\n" . (string)$summary['summary'];
	}

	/** @param list<float> $left @param list<float> $right */
	public static function cosine(array $left, array $right): float {
		if ($left === [] || count($left) !== count($right)) {
			return -1.0;
		}
		$dot = $ln = $rn = 0.0;
		foreach ($left as $index => $value) {
			$l = (float)$value;
			$r = (float)$right[$index];
			$dot += $l * $r;
			$ln += $l * $l;
			$rn += $r * $r;
		}
		return $ln > 0 && $rn > 0 ? max(-1.0, min(1.0, $dot / sqrt($ln * $rn))) : -1.0;
	}
}
