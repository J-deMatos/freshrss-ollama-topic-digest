<?php
declare(strict_types=1);

require_once __DIR__ . '/TopicDigestStore.php';
require_once __DIR__ . '/ArticleSummaryCache.php';
require_once __DIR__ . '/TopicDigestOllama.php';
require_once __DIR__ . '/TopicDigestScraper.php';
require_once __DIR__ . '/TopicDigestProcessor.php';
require_once __DIR__ . '/TopicDigestCloudConcurrency.php';
require_once __DIR__ . '/TopicDigestCoordinator.php';

/**
 * @phpstan-type TopicDigestConfig array{ollama_url:string,model_profile:string,summary_model:string,
 *  judge_model:string,embedding_model:string,local_summary_model:string,local_judge_model:string,
 *  local_embedding_model:string,cloud_summary_model:string,cloud_judge_model:string,cloud_embedding_model:string,
 *  timeout:int,scraping:bool,always_show_topics:bool,cloud_concurrency:string}
 */
final class TopicDigestExtension extends Minz_Extension {
	private const CATEGORY_NAME = 'Topic Digests';
	private const DEFAULT_OLLAMA_TIMEOUT = 1800;
	private const LEGACY_OLLAMA_TIMEOUT = 180;
	private const MAX_OLLAMA_TIMEOUT = 7200;
	private ?TopicDigestStore $storeInstance = null;
	private ?FreshRSS_ArticleSummaryCache $summaryCacheInstance = null;
	private bool $workerLaunchAttempted = false;
	private bool $suppressManualUnreadTracking = false;
	/** @var array<int,true>|null */
	private ?array $syntheticFeedIds = null;
	/** @var array<string,true> */
	private array $decorated = [];
	/** @var list<array<string,mixed>> */
	public array $topics = [];
	/** @var array<string,mixed>|null */
	public ?array $editTopic = null;
	/** @var array<int,FreshRSS_Category> */
	public array $categories = [];
	/** @var array<int,FreshRSS_Feed> */
	public array $feeds = [];
	/** @var TopicDigestConfig */
	public array $settings = [];
	/** @var array<string,int|float|string> */
	public array $status = [];
	/** @var list<array<string,mixed>> */
	public array $errors = [];

	#[\Override]
	public function init(): void {
		parent::init();
		$this->registerTranslates();
		$this->registerController('topicDigest');
		// EntryBeforeAdd runs after FreshRSS applies feed filters. Fall back for older releases.
		if (defined(Minz_HookType::class . '::EntryBeforeAdd')) {
			// Queue Topic Digest before News Deduplicator (which uses the default priority).
			$this->registerHook(constant(Minz_HookType::class . '::EntryBeforeAdd'), [$this, 'enqueueEntry'], -100);
		} else {
			$this->registerHook(Minz_HookType::EntryBeforeInsert, [$this, 'enqueueEntry'], -100);
		}
		$this->registerHook(Minz_HookType::EntryBeforeUpdate, [$this, 'enqueueEntry'], -100);
		if ($this->supportsEntriesReadHook()) {
			$this->registerHook(constant(Minz_HookType::class . '::EntriesRead'), [$this, 'entriesRead']);
		}
		if (defined(Minz_HookType::class . '::FeedsListBeforeActualize')) {
			$this->registerHook(constant(Minz_HookType::class . '::FeedsListBeforeActualize'),
				[$this, 'filterActualization']);
		}
		if (defined(Minz_HookType::class . '::FeedBeforeActualize')) {
			$this->registerHook(constant(Minz_HookType::class . '::FeedBeforeActualize'),
				[$this, 'filterSyntheticFeed']);
		}
		$this->registerHook(Minz_HookType::EntryBeforeDisplay, [$this, 'decorateEntry']);
		if (defined(Minz_HookType::class . '::NavMenu')) {
			$this->registerHook(constant(Minz_HookType::class . '::NavMenu'), [$this, 'renderGlobalStats']);
		}
		if (defined(Minz_HookType::class . '::JsVars')) {
			$this->registerHook(constant(Minz_HookType::class . '::JsVars'), [$this, 'javascriptVars']);
		}
		Minz_View::appendStyle($this->getFileUrl('topic-digest.css'));
		Minz_View::appendScript($this->getFileUrl('topic-digest.js'), false, true, false);
		Minz_View::appendStyle($this->getFileUrl('status.css'));
		Minz_View::appendScript($this->getFileUrl('status.js'), false, true, false);
		if (!defined('TOPIC_DIGEST_CHILD_WORKER') && Minz_Request::paramString('topic_digest_action') !== 'stats') {
			$this->store()->ensureClassifierRevision('topic-context-v2');
			$this->reconcileMissingTopics();
			$this->launchAutomaticWorker();
		}
	}

	private function reconcileMissingTopics(): void {
		try {
			$feedDao = FreshRSS_Factory::createFeedDao();
			$entryDao = FreshRSS_Factory::createEntryDao();
			foreach ($this->store()->topics() as $topic) {
				$topicLock = $this->acquireTopicLock((int)$topic['id']);
				try {
					$feedId = (int)($topic['feed_id'] ?? 0);
					$entryId = (string)($topic['entry_id'] ?? '');
					$feed = $feedId > 0 ? $feedDao->searchById($feedId) : null;
					$shouldMaterialise = $this->topicHasSyntheticFeed($topic);
					if (!$shouldMaterialise) {
						if ($feedId > 0 || $entryId !== '') {
							$this->synchroniseTopic((int)$topic['id'], false);
						}
						continue;
					}
					$overviewEntryMissing = $entryId === '' || $entryDao->searchById($entryId) === null;
					$feedTopicAttributes = $feed?->attributeArray('topic_digest') ?? [];
					if ($feedId < 1 || $feed === null || $overviewEntryMissing
							|| $feed->priority() !== FreshRSS_Feed::PRIORITY_CATEGORY
							|| ($feedTopicAttributes['topic_type'] ?? null) !== $topic['topic_type']) {
						$this->synchroniseTopic((int)$topic['id'], false);
					}
				} finally {
					flock($topicLock, LOCK_UN);
					fclose($topicLock);
				}
			}
		} catch (Throwable $e) {
			Minz_Log::error('Topic Digest reconciliation error: ' . $e->getMessage());
		}
	}

	#[\Override]
	public function install(): true|string {
		try {
			$this->store();
			return true;
		} catch (Throwable $e) {
			return $e->getMessage();
		}
	}

	public function store(): TopicDigestStore {
		return $this->storeInstance ??= new TopicDigestStore($this->getExtensionUserPath() . '/topic-digest.sqlite');
	}

	public function sharedSummaryCache(): FreshRSS_ArticleSummaryCache {
		$username = Minz_User::name() ?: '_';
		return $this->summaryCacheInstance ??= new FreshRSS_ArticleSummaryCache(
			USERS_PATH . "/{$username}/extensions/article-summaries.sqlite"
		);
	}

	/** @return TopicDigestConfig */
	public function configuration(): array {
		$legacySummary = $this->getUserConfigurationString('summary_model');
		$legacyJudge = $this->getUserConfigurationString('judge_model');
		$legacyEmbedding = $this->getUserConfigurationString('embedding_model');
		$legacyIsCloud = $legacySummary !== null && $legacyJudge !== null
			&& str_ends_with($legacySummary, ':cloud') && str_ends_with($legacyJudge, ':cloud');
		$profile = $this->getUserConfigurationString('model_profile');
		if (!in_array($profile, ['local', 'cloud'], true)) {
			$profile = $legacyIsCloud ? 'cloud' : 'local';
		}
		$localSummary = $this->getUserConfigurationString('local_summary_model')
			?? (!$legacyIsCloud && $legacySummary !== null ? $legacySummary : 'qwen3.5:4b');
		$localJudge = $this->getUserConfigurationString('local_judge_model')
			?? (!$legacyIsCloud && $legacyJudge !== null ? $legacyJudge : 'qwen3.5:9b');
		$localEmbedding = $this->getUserConfigurationString('local_embedding_model')
			?? $legacyEmbedding ?? 'qwen3-embedding:0.6b';
		$cloudSummary = $this->getUserConfigurationString('cloud_summary_model')
			?? ($legacyIsCloud && $legacySummary !== null ? $legacySummary : 'gpt-oss:20b-cloud');
		$cloudJudge = $this->getUserConfigurationString('cloud_judge_model')
			?? ($legacyIsCloud && $legacyJudge !== null ? $legacyJudge : 'gpt-oss:20b-cloud');
		$cloudEmbedding = $this->getUserConfigurationString('cloud_embedding_model')
			?? $legacyEmbedding ?? 'qwen3-embedding:0.6b';
		return [
			'ollama_url' => $this->getUserConfigurationString('ollama_url') ?? 'http://ollama:11434',
			'model_profile' => $profile,
			'summary_model' => $profile === 'cloud' ? $cloudSummary : $localSummary,
			'judge_model' => $profile === 'cloud' ? $cloudJudge : $localJudge,
			'embedding_model' => $profile === 'cloud' ? $cloudEmbedding : $localEmbedding,
			'local_summary_model' => $localSummary,
			'local_judge_model' => $localJudge,
			'local_embedding_model' => $localEmbedding,
			'cloud_summary_model' => $cloudSummary,
			'cloud_judge_model' => $cloudJudge,
			'cloud_embedding_model' => $cloudEmbedding,
			'timeout' => $this->ollamaTimeoutConfiguration(),
			'scraping' => $this->getUserConfigurationBool('scraping') ?? true,
			'always_show_topics' => $this->getUserConfigurationBool('always_show_topics') ?? true,
			'cloud_concurrency' => $this->cloudConcurrencyConfiguration(),
		];
	}

	private function cloudConcurrencyConfiguration(): string {
		$value = $this->getUserConfigurationString('cloud_concurrency') ?? 'auto';
		return in_array($value, ['auto', '1', '2', '4', '8', '16'], true) ? $value : 'auto';
	}

	private function ollamaTimeoutConfiguration(): int {
		$timeout = $this->getUserConfigurationInt('timeout');
		if ($timeout === null || $timeout === self::LEGACY_OLLAMA_TIMEOUT) {
			return self::DEFAULT_OLLAMA_TIMEOUT;
		}
		return min(self::MAX_OLLAMA_TIMEOUT, max(10, $timeout));
	}

	public function pipelineHash(): string {
		$config = $this->configuration();
		$topics = array_map(static fn(array $topic): array => [
			$topic['id'], $topic['rule_hash'], $topic['enabled'], $topic['all_feeds'], $topic['all_categories'],
			$topic['feed_ids'], $topic['category_ids'], $topic['backfill_mode'], $topic['backfill_days'], $topic['topic_type'],
			$topic['show_verification'],
		], $this->store()->topics());
		return hash('sha256', json_encode([$config['summary_model'], $config['judge_model'],
			$config['embedding_model'], $config['scraping'], $this->store()->pipelineRevision(), $topics], JSON_THROW_ON_ERROR));
	}

	public function analysisHash(): string {
		$config = $this->configuration();
		return hash('sha256', json_encode([$config['summary_model'], $config['embedding_model'], $config['scraping']],
			JSON_THROW_ON_ERROR));
	}

	public function lockPath(): string {
		return $this->getExtensionUserPath() . '/worker.lock';
	}

	/** @param (Closure():void)|null $heartbeat @return resource */
	public function acquireTopicLock(int $topicId, ?Closure $heartbeat = null) {
		$lock = fopen($this->getExtensionUserPath() . '/topic-' . $topicId . '.lock', 'c');
		if ($lock === false) {
			throw new RuntimeException('Cannot acquire the Topic Digest topic lock.');
		}
		try {
			$lastHeartbeat = hrtime(true);
			while (!flock($lock, LOCK_EX | LOCK_NB)) {
				if ($heartbeat !== null && hrtime(true) - $lastHeartbeat >= 30_000_000_000) {
					$heartbeat();
					$lastHeartbeat = hrtime(true);
				}
				usleep(100000);
			}
		} catch (Throwable $e) {
			fclose($lock);
			throw $e;
		}
		return $lock;
	}

	public function enqueueEntry(FreshRSS_Entry $entry): FreshRSS_Entry {
		if ($this->isSyntheticFeed($entry->feedId()) || $this->store()->topics(true) === []) {
			return $entry;
		}
		try {
			$this->store()->enqueue($entry, $entry->feed()?->categoryId() ?? 0, $this->pipelineHash(),
				TopicDigestStore::livePriority(), 0);
			$this->launchAutomaticWorker();
		} catch (Throwable $e) {
			Minz_Log::error('Topic Digest queue error: ' . $e->getMessage());
		}
		return $entry;
	}

	public function shouldBlockNewsDeduplicator(FreshRSS_Entry $entry): bool {
		if ($this->store()->isPaused() || $this->store()->topics(true) === []
				|| $this->isSyntheticFeed($entry->feedId())) {
			return false;
		}
		return $this->store()->classificationPending($entry->id(), $entry->hash());
	}

	/** @param list<numeric-string> $ids */
	public function entriesRead(array $ids, bool $isRead): void {
		if ($this->suppressManualUnreadTracking) {
			return;
		}
		foreach ($ids as $id) {
			$entry = FreshRSS_Factory::createEntryDao()->searchById((string)$id);
			if ($this->isDigestEntry((string)$id) || ($entry !== null && $this->isSyntheticFeed($entry->feedId()))) {
				continue;
			}
			if ($isRead) {
				$this->store()->clearManualUnread((string)$id);
				$this->store()->clearRebuildUnread((string)$id);
			} else {
				$this->store()->recordManualUnread((string)$id);
			}
		}
	}

	public function supportsEntriesReadHook(): bool {
		return defined(Minz_HookType::class . '::EntriesRead');
	}

	public function isSyntheticFeed(int $feedId): bool {
		if ($this->syntheticFeedIds === null) {
			$this->syntheticFeedIds = [];
			foreach ($this->store()->topics() as $topic) {
				if ((int)($topic['feed_id'] ?? 0) > 0) {
					$this->syntheticFeedIds[(int)$topic['feed_id']] = true;
				}
			}
		}
		return isset($this->syntheticFeedIds[$feedId]);
	}

	private function isDigestEntry(string $entryId): bool {
		foreach ($this->store()->topics() as $topic) {
			if ((string)($topic['entry_id'] ?? '') === $entryId) {
				return true;
			}
		}
		return false;
	}

	/** @param iterable<FreshRSS_Feed> $feeds @return list<FreshRSS_Feed> */
	public function filterActualization(iterable $feeds): array {
		$list = is_array($feeds) ? $feeds : iterator_to_array($feeds);
		return array_values(array_filter($list, fn(FreshRSS_Feed $feed): bool => !$this->isSyntheticFeed($feed->id())));
	}

	public function filterSyntheticFeed(FreshRSS_Feed $feed): ?FreshRSS_Feed {
		return $this->isSyntheticFeed($feed->id()) ? null : $feed;
	}

	public function enqueueBackfillPage(int $pageSize): int {
		$backfill = $this->store()->backfill();
		if (!$backfill['active']) {
			return 0;
		}
		$limit = max(1, min(100, $pageSize));
		$feedCategories = [];
		foreach (FreshRSS_Factory::createFeedDao()->selectAll() as $feed) {
			$feedCategories[(int)$feed['id']] = (int)$feed['category'];
		}
		$count = 0;
		$lastId = null;
		$pipelineHash = $this->pipelineHash();
		foreach (FreshRSS_Factory::createEntryDao()->listWhere(
				id_max: $backfill['cursor'], sort: 'id', order: 'DESC', limit: $limit) as $entry) {
			$lastId = $entry->id();
			if (!$this->isSyntheticFeed($entry->feedId())) {
				$this->store()->enqueue($entry, $feedCategories[$entry->feedId()] ?? 0, $pipelineHash, 10, archive: true);
			}
			$count++;
		}
		if ($count === 0) {
			$this->store()->advanceBackfill($backfill['cursor'], false);
			return 0;
		}
		$cursor = $lastId === null ? $backfill['cursor'] : self::previousNumericString($lastId);
		$this->store()->advanceBackfill($cursor, $count === $limit);
		return $count;
	}

	private static function previousNumericString(string $value): string {
		$digits = str_split($value);
		for ($index = count($digits) - 1; $index >= 0; $index--) {
			if ($digits[$index] !== '0') {
				$digits[$index] = (string)(((int)$digits[$index]) - 1);
				break;
			}
			$digits[$index] = '9';
		}
		return ltrim(implode('', $digits), '0') ?: '0';
	}

	public function synchroniseTopic(int $topicId, bool $markUnread, bool $prune = false): void {
		$topic = $this->store()->topic($topicId);
		if ($topic === null) {
			throw new InvalidArgumentException('Unknown topic.');
		}
		$feedDao = FreshRSS_Factory::createFeedDao();
		$syntheticUrl = 'https://topic-digest.invalid/topic/' . $topicId;
		$feed = (int)($topic['feed_id'] ?? 0) > 0 ? $feedDao->searchById((int)$topic['feed_id']) : null;
		$feed ??= $feedDao->searchByUrl($syntheticUrl);
		if (!$this->topicHasSyntheticFeed($topic)) {
			if ($feed !== null && !FreshRSS_feed_Controller::deleteFeed($feed->id())) {
				throw new RuntimeException('Could not remove the hidden mark-read verification feed.');
			}
			$this->store()->attachSynthetic($topicId, null, null);
			$this->syntheticFeedIds = null;
			FreshRSS_UserDAO::touch();
			return;
		}
		$categoryId = $this->ensureCategory();
		$description = $this->feedDescription($topic);
		$feedTopicAttributes = $feed?->attributeArray('topic_digest') ?? [];
		$currentType = (string)($feedTopicAttributes['topic_type'] ?? 'digest');
		$preservedOverview = null;
		if ($feed !== null && $currentType !== $topic['topic_type']) {
			$preservedOverview = FreshRSS_Factory::createEntryDao()->searchByGuid(
				$feed->id(), $this->topicOverviewGuid($topicId)
			);
			if (!FreshRSS_feed_Controller::deleteFeed($feed->id())) {
				throw new RuntimeException('Could not replace the synthetic Topic Digest feed after changing its type.');
			}
			$this->store()->attachSynthetic($topicId, null, null);
			$this->syntheticFeedIds = null;
			$feed = null;
		}
		$attributes = $feed?->attributes() ?? [];
		$attributes['topic_digest'] = [
			'topic_id' => (string)$topicId,
			'topic_type' => (string)$topic['topic_type'],
		];
		if ($feed === null) {
			$feedId = $feedDao->addFeed([
				'url' => $syntheticUrl,
				'kind' => FreshRSS_Feed::KIND_RSS,
				'category' => $categoryId,
				'name' => (string)$topic['name'],
				'website' => '',
				'description' => $description,
				'lastUpdate' => time(),
				'priority' => FreshRSS_Feed::PRIORITY_CATEGORY,
				'error' => 0,
				'ttl' => -86400,
				'attributes' => $attributes,
			]);
			if ($feedId === false) {
				throw new RuntimeException('Could not create the synthetic Topic Digest feed.');
			}
			$feed = $feedDao->searchById($feedId);
			$this->syntheticFeedIds = null;
		} else {
			$feedId = $feed->id();
			if (!$feedDao->updateFeed($feedId, ['name' => (string)$topic['name'], 'category' => $categoryId,
				'description' => $description, 'priority' => FreshRSS_Feed::PRIORITY_CATEGORY,
				'attributes' => $attributes])) {
				throw new RuntimeException('Could not update the synthetic Topic Digest feed.');
			}
		}
		if ($feed === null) {
			throw new RuntimeException('Could not reload the synthetic Topic Digest feed.');
		}
		if ($topic['topic_type'] === 'feed') {
			$detachedOverview = $this->synchroniseHighPriorityFeed($topic, $feedId, $prune);
			$overview = $this->synchroniseOverviewEntry(
				$topic, $feedId, $markUnread, $preservedOverview ?? $detachedOverview, true
			);
			$this->store()->attachSynthetic($topicId, $feedId, $overview->id());
			$feedDao->updateCachedValues($feedId);
			FreshRSS_UserDAO::touch();
			$this->syntheticFeedIds = null;
			return;
		}
		$entry = $this->synchroniseOverviewEntry($topic, $feedId, $markUnread, $preservedOverview);
		$this->store()->attachSynthetic($topicId, $feedId, $entry->id());
		$feedDao->updateCachedValues($feedId);
		FreshRSS_UserDAO::touch();
		$this->syntheticFeedIds = null;
	}

	/** @param array<string,mixed> $topic */
	private function topicHasSyntheticFeed(array $topic): bool {
		return $topic['topic_type'] !== 'mark_read' || (bool)$topic['show_verification'];
	}

	/** @param array<string,mixed> $topic */
	private function synchroniseHighPriorityFeed(array $topic, int $feedId, bool $prune): ?FreshRSS_Entry {
		$entryDao = FreshRSS_Factory::createEntryDao();
		$sourceIds = [];
		foreach ($this->store()->events((int)$topic['id']) as $event) {
			foreach ($event['sources'] as $source) {
				$sourceIds[] = (string)$source['entry_id'];
			}
		}
		$existing = [];
		$detachedOverview = null;
		if ($prune) {
			foreach ($entryDao->listWhere(type: 'f', id: $feedId, state: FreshRSS_Entry::STATE_ALL, limit: -1) as $entry) {
				if ($entry->guid() === $this->topicOverviewGuid((int)$topic['id'])) {
					$detachedOverview = $entry;
				} else {
					$existing[$entry->guid()] = $entry;
				}
			}
			if ($entryDao->cleanOldEntries($feedId) === false) {
				throw new RuntimeException('Could not reconcile the high-priority Topic Digest feed.');
			}
		}
		foreach (array_values(array_unique($sourceIds)) as $sourceId) {
			$guid = $this->topicSourceGuid((int)$topic['id'], $sourceId);
			$this->materialiseHighPriorityEntry($topic, $feedId, $sourceId, $existing[$guid] ?? null);
		}
		return $detachedOverview;
	}

	public function materialiseTopicSource(int $topicId, string $sourceEntryId, bool $markOverviewUnread): void {
		$topic = $this->store()->topic($topicId);
		if ($topic === null || $topic['topic_type'] !== 'feed') {
			return;
		}
		$feedId = (int)($topic['feed_id'] ?? 0);
		if ($feedId < 1 || FreshRSS_Factory::createFeedDao()->searchById($feedId) === null) {
			$this->synchroniseTopic($topicId, false);
			return;
		}
		$this->materialiseHighPriorityEntry($topic, $feedId, $sourceEntryId);
		$overview = $this->synchroniseOverviewEntry($topic, $feedId, $markOverviewUnread, pinned: true);
		$this->store()->attachSynthetic($topicId, $feedId, $overview->id());
		FreshRSS_Factory::createFeedDao()->updateCachedValues($feedId);
		FreshRSS_UserDAO::touch();
	}

	/** @param array<string,mixed> $topic */
	private function materialiseHighPriorityEntry(array $topic, int $feedId, string $sourceEntryId,
		?FreshRSS_Entry $previous = null): void {
		$entryDao = FreshRSS_Factory::createEntryDao();
		$guid = $this->topicSourceGuid((int)$topic['id'], $sourceEntryId);
		$previous ??= $entryDao->searchByGuid($feedId, $guid);
		$source = $entryDao->searchById($sourceEntryId);
		if ($source === null && $previous === null) {
			return;
		}
		$values = ($source ?? $previous)->toArray();
		$attributes = is_array($values['attributes'] ?? null) ? $values['attributes'] : [];
		$attributes['topic_digest'] = [
			'topic_id' => (string)$topic['id'],
			'topic_type' => 'feed',
			'source_entry_id' => $sourceEntryId,
		];
		$values['id'] = $previous?->id() ?? uTimeString();
		$values['guid'] = $guid;
		$values['id_feed'] = $feedId;
		$values['attributes'] = $attributes;
		$values['is_read'] = $previous?->isRead() ?? false;
		$values['is_favorite'] = $previous?->isFavorite() ?? false;
		$values['lastUserModified'] = $previous?->lastUserModified();
		if ($previous === null) {
			if (!$entryDao->addEntry($values, false)) {
				throw new RuntimeException('Could not add an article to the high-priority Topic Digest feed.');
			}
		} elseif (!$entryDao->updateEntry($values)) {
			throw new RuntimeException('Could not update an article in the high-priority Topic Digest feed.');
		}
	}

	private function topicSourceGuid(int $topicId, string $sourceEntryId): string {
		return 'topic-digest-source:' . $topicId . ':' . hash('sha256', $sourceEntryId);
	}

	/** @param array<string,mixed> $topic */
	private function synchroniseOverviewEntry(array $topic, int $feedId, bool $markUnread,
		?FreshRSS_Entry $detached = null, bool $pinned = false): FreshRSS_Entry {
		$entryDao = FreshRSS_Factory::createEntryDao();
		$guid = $this->topicOverviewGuid((int)$topic['id']);
		$entry = $detached ?? $entryDao->searchByGuid($feedId, $guid);
		$events = $this->store()->events((int)$topic['id']);
		$date = $this->overviewDate($events, $pinned);
		$content = $this->renderDigest($topic, false);
		if ($entry === null) {
			$startsUnread = $markUnread || ($pinned && $events !== []);
			$entry = new FreshRSS_Entry($feedId, $guid, (string)$topic['name'], 'Topic Digest', $content,
				'https://topic-digest.invalid/topic/' . $topic['id'], $date, !$startsUnread, false);
			$entry->_id(uTimeString());
			$entry->_lastSeen(time());
			if (!$entryDao->addEntry($entry->toArray(), false)) {
				throw new RuntimeException('Could not create the living Topic Digest overview entry.');
			}
			return $entry;
		}
		$entry->_title((string)$topic['name']);
		$entry->_content($content);
		$entry->_date($date);
		$entry->_lastModified(time());
		if ($markUnread) {
			$entry->_isRead(false);
		}
		$values = $entry->toArray();
		$values['id_feed'] = $feedId;
		if ($detached !== null) {
			if (!$entryDao->addEntry($values, false)) {
				throw new RuntimeException('Could not restore the living Topic Digest overview entry.');
			}
		} elseif (!$entryDao->updateEntry($values)) {
			throw new RuntimeException('Could not update the living Topic Digest overview entry.');
		}
		return $entry;
	}

	/** @param list<array<string,mixed>> $events */
	private function overviewDate(array $events, bool $pinned): int {
		if ($events === []) {
			return time();
		}
		if (!$pinned) {
			return (int)$events[0]['occurred_at'];
		}
		$newest = 0;
		foreach ($events as $event) {
			$newest = max($newest, (int)$event['occurred_at']);
			foreach ($event['sources'] as $source) {
				$newest = max($newest, (int)$source['published_at']);
			}
		}
		return $newest + 1;
	}

	private function topicOverviewGuid(int $topicId): string {
		return 'topic-digest:' . $topicId;
	}

	private function ensureCategory(): int {
		$dao = FreshRSS_Factory::createCategoryDao();
		foreach ($dao->listCategories(prePopulateFeeds: false) as $category) {
			if ($category->attributeBoolean('topic_digest') === true) {
				return $category->id();
			}
		}
		$category = $dao->searchByName(self::CATEGORY_NAME);
		if ($category !== null) {
			$attributes = $category->attributes();
			$attributes['topic_digest'] = true;
			$dao->updateCategory($category->id(), [
				'name' => html_entity_decode($category->name(), ENT_QUOTES | ENT_HTML5, 'UTF-8'),
				'kind' => $category->kind(),
				'attributes' => $attributes,
			]);
			return $category->id();
		}
		$id = $dao->addCategory(['name' => self::CATEGORY_NAME, 'attributes' => ['topic_digest' => true]]);
		if ($id === false) {
			throw new RuntimeException('Could not create the Topic Digests category.');
		}
		return $id;
	}

	/**
	 * Make the extension-owned category open all living digests by default.
	 *
	 * @param array<string,mixed> $vars
	 * @return array<string,mixed>
	 */
	public function javascriptVars(array $vars): array {
		try {
			foreach (FreshRSS_Factory::createCategoryDao()->listCategories(prePopulateFeeds: false) as $category) {
				if ($category->attributeBoolean('topic_digest') === true) {
					$vars['topic_digest'] = [
						'category_id' => $category->id(),
						'all_state' => FreshRSS_Entry::STATE_ALL,
						'feed_counts' => $this->topicFeedCounts(),
						'feed_types' => $this->topicFeedTypes(),
						'always_show_topics' => $this->configuration()['always_show_topics'],
					];
					break;
				}
			}
		} catch (Throwable $e) {
			Minz_Log::error('Topic Digest JavaScript variables error: ' . $e->getMessage());
		}
		return $vars;
	}

	/** @return array<int,int> Synthetic feed ID to aggregated source-article count. */
	private function topicFeedCounts(): array {
		$counts = [];
		foreach ($this->store()->topics() as $topic) {
			$feedId = (int)($topic['feed_id'] ?? 0);
			if ($feedId > 0) {
				$counts[$feedId] = (int)$topic['article_count'];
			}
		}
		return $counts;
	}

	/** @return array<int,string> Synthetic feed ID to digest or feed presentation type. */
	private function topicFeedTypes(): array {
		$types = [];
		foreach ($this->store()->topics() as $topic) {
			$feedId = (int)($topic['feed_id'] ?? 0);
			if ($feedId > 0) {
				$types[$feedId] = (string)$topic['topic_type'];
			}
		}
		return $types;
	}

	/** @param array<string,mixed> $topic */
	private function feedDescription(array $topic): string {
		$text = (string)$topic['description'];
		if ($topic['exclusions'] !== []) {
			$text .= "\n\n" . _t('ext.topic_digest.exclusions') . ': ' . implode('; ', $topic['exclusions']);
		}
		$behaviour = match ($topic['topic_type']) {
			'feed' => _t('ext.topic_digest.high_priority_feed_behaviour'),
			'mark_read' => _t('ext.topic_digest.mark_read_feed_behaviour'),
			default => _t('ext.topic_digest.feed_behaviour'),
		};
		return $text . "\n\n" . $behaviour;
	}

	public function decorateEntry(FreshRSS_Entry $entry): FreshRSS_Entry {
		if (isset($this->decorated[$entry->id()])) {
			return $entry;
		}
		if (!$this->supportsEntriesReadHook() && !$entry->isRead() && !$this->isSyntheticFeed($entry->feedId())
				&& method_exists($entry, 'lastUserModified') && $entry->lastUserModified() !== null
				&& !$this->store()->isProtected($entry->id()) && !$this->store()->isRebuildUnread($entry->id())) {
			$this->store()->recordManualUnread($entry->id());
		}
		$topicAttributes = $entry->attributeArray('topic_digest') ?? [];
		if (($topicAttributes['topic_type'] ?? null) === 'feed') {
			$topicId = (int)($topicAttributes['topic_id'] ?? 0);
			$sourceEntryId = (string)($topicAttributes['source_entry_id'] ?? '');
			$source = $topicId > 0 && $sourceEntryId !== '' ? $this->store()->source($topicId, $sourceEntryId) : null;
			if ($source !== null && Minz_Request::controllerName() === 'index' && Minz_Request::actionName() !== 'rss') {
				$esc = static fn(string $value): string => htmlspecialchars($value, ENT_QUOTES, 'UTF-8');
				$footer = '<aside class="topic-digest-high-priority-controls"><details><summary>'
					. $esc(_t('ext.topic_digest.why_matched')) . '</summary><p>'
					. $esc((string)$source['explanation']) . '</p></details>'
					. $this->actionForm('restoreSource', $topicId, _t('ext.topic_digest.restore'), [
						'entry_id' => $sourceEntryId,
					]) . '</aside>';
				$entry->_content($entry->content(false) . $footer);
			}
			$this->decorated[$entry->id()] = true;
			return $entry;
		}
		foreach ($this->store()->topics() as $topic) {
			if ((string)($topic['entry_id'] ?? '') === $entry->id()) {
				$interactive = Minz_Request::controllerName() === 'index' && Minz_Request::actionName() !== 'rss';
				$entry->_content($this->renderDigest($topic, $interactive));
				$this->decorated[$entry->id()] = true;
				break;
			}
		}
		return $entry;
	}

	/** @param array<string,mixed> $topic */
	private function renderDigest(array $topic, bool $interactive): string {
		$esc = static fn(string $value): string => htmlspecialchars($value, ENT_QUOTES, 'UTF-8');
		$html = '<section class="topic-digest" data-topic-digest="1"><p class="topic-digest-description">'
			. nl2br($esc((string)$topic['description'])) . '</p>';
		if ($topic['exclusions'] !== []) {
			$html .= '<p><strong>' . $esc(_t('ext.topic_digest.exclusions')) . ':</strong> '
				. $esc(implode('; ', $topic['exclusions'])) . '</p>';
		}
		$html .= '<ol class="topic-digest-events">';
		foreach ($this->store()->events((int)$topic['id']) as $event) {
			$html .= '<li><article><header><span class="topic-digest-date-label">'
				. $esc(_t('ext.topic_digest.effective_date')) . ':</span> <time datetime="'
				. $esc(date(DATE_ATOM, (int)$event['occurred_at'])) . '">'
				. $esc(date('Y-m-d', (int)$event['occurred_at'])) . '</time> — <strong>' . $esc((string)$event['title'])
				. '</strong></header><ul>';
			foreach ($event['sources'] as $source) {
				$url = $this->safeUrl((string)$source['link']);
				if ($url === null) {
					continue;
				}
				$html .= '<li><a href="' . $esc($url) . '" rel="noopener noreferrer">' . $esc((string)$source['title'])
					. '</a> — ' . $esc((string)$source['feed_name']) . ' · '
					. $esc(_t('ext.topic_digest.published')) . ': '
					. $esc(date('Y-m-d H:i', (int)$source['published_at']));
				if ($interactive) {
					$html .= $this->actionForm('restoreSource', (int)$topic['id'], _t('ext.topic_digest.restore'), [
						'entry_id' => (string)$source['entry_id'],
					]);
				}
				$html .= '<details><summary>' . $esc(_t('ext.topic_digest.why_matched')) . '</summary><p>'
					. $esc((string)$source['explanation'])
					. '</p></details></li>';
			}
			$html .= '</ul>';
			if ($interactive) {
				$html .= $this->actionForm('restoreEvent', (int)$topic['id'], _t('ext.topic_digest.restore_all'),
					['event_id' => (string)$event['id']]);
			}
			$html .= '</article></li>';
		}
		return $html . '</ol></section>';
	}

	/** @param array<string,string> $parameters */
	private function actionForm(string $action, int $topicId, string $label, array $parameters): string {
		$url = _url('topicDigest', $action);
		$html = '<form class="topic-digest-action" method="post" action="' . htmlspecialchars($url, ENT_QUOTES, 'UTF-8') . '">'
			. '<input type="hidden" name="_csrf" value="' . htmlspecialchars(FreshRSS_Auth::csrfToken(), ENT_QUOTES, 'UTF-8') . '" />'
			. '<input type="hidden" name="topic_id" value="' . $topicId . '" />';
		foreach ($parameters as $key => $value) {
			$html .= '<input type="hidden" name="' . htmlspecialchars($key, ENT_QUOTES, 'UTF-8') . '" value="'
				. htmlspecialchars($value, ENT_QUOTES, 'UTF-8') . '" />';
		}
		return $html . '<button type="submit" class="btn btn-mini">' . htmlspecialchars($label, ENT_QUOTES, 'UTF-8')
			. '</button></form>';
	}

	private function safeUrl(string $url): ?string {
		$parts = parse_url($url);
		return is_array($parts) && in_array(strtolower((string)($parts['scheme'] ?? '')), ['http', 'https'], true)
			&& !empty($parts['host']) && !isset($parts['user']) && !isset($parts['pass']) ? $url : null;
	}

	/** @return list<string> */
	public function restoreSource(int $topicId, string $entryId): array {
		$topicLock = $this->acquireTopicLock($topicId);
		try {
			$ids = $this->store()->restoreSource($topicId, $entryId);
			$this->markUnread($ids);
			$this->synchroniseTopic($topicId, false, true);
			return $ids;
		} finally {
			flock($topicLock, LOCK_UN);
			fclose($topicLock);
		}
	}

	/** @return list<string> */
	public function restoreEvent(int $topicId, int $eventId): array {
		$topicLock = $this->acquireTopicLock($topicId);
		try {
			$ids = $this->store()->restoreEvent($topicId, $eventId);
			$this->markUnread($ids);
			$this->synchroniseTopic($topicId, false, true);
			return $ids;
		} finally {
			flock($topicLock, LOCK_UN);
			fclose($topicLock);
		}
	}

	/** @param list<string> $ids */
	private function markUnread(array $ids): void {
		if ($ids !== [] && FreshRSS_Factory::createEntryDao()->markRead($ids, false) === false) {
			throw new RuntimeException('Could not restore the original articles to unread.');
		}
	}

	public function finishRebuildForEntry(FreshRSS_Entry $entry, bool $matched): void {
		if (!$this->store()->isPendingRebuildRestore($entry->id())) {
			return;
		}
		if ($matched) {
			$this->store()->clearRebuildUnread($entry->id());
			$this->store()->completeRebuildRestore($entry->id());
			return;
		}
		if ($entry->isRead()) {
			$this->suppressManualUnreadTracking = true;
			try {
				if (FreshRSS_Factory::createEntryDao()->markRead($entry->id(), false) === false) {
					throw new RuntimeException('Could not restore a removed Topic Digest source to unread.');
				}
			} finally {
				$this->suppressManualUnreadTracking = false;
			}
		}
		$this->store()->recordRebuildUnread($entry->id());
		$this->store()->completeRebuildRestore($entry->id());
	}

	public function renderGlobalStats(): string {
		try {
			$payload = $this->formattedStatistics();
			$esc = static fn(string $value): string => htmlspecialchars($value, ENT_QUOTES, 'UTF-8');
			$settingsUrl = htmlspecialchars_decode(
				_url('extension', 'configure', 'e', urlencode($this->getName())),
				ENT_QUOTES
			);
			$statsUrl = htmlspecialchars_decode(_url(
				'extension', 'configure',
				'e', urlencode($this->getName()),
				'topic_digest_action', 'stats'
			), ENT_QUOTES);
			$rows = '';
			foreach ($payload['items'] as $key => $item) {
				$rows .= '<dt>' . $esc($item['label']) . '</dt><dd data-topic-digest-stat="' . $esc($key) . '">'
					. $esc($item['value']) . '</dd>';
			}
			return '<details class="topic-digest-global-stats" data-topic-digest-stats="1" data-stats-url="'
				. $esc($statsUrl) . '" data-control-url="' . $esc($settingsUrl) . '" data-csrf="'
				. $esc(FreshRSS_Auth::csrfToken()) . '"><summary class="btn" data-topic-digest-stat-summary="1">'
				. $esc($payload['summary']) . '</summary><div class="topic-digest-global-stats-panel"><dl>'
				. $rows . '</dl><a href="' . $esc($settingsUrl) . '">'
				. $esc(_t('ext.topic_digest.open_settings')) . '</a><button type="button" class="btn '
				. 'topic-digest-worker-toggle" data-topic-digest-toggle="1" data-action="'
				. $esc($payload['control']['action']) . '">' . $esc($payload['control']['label'])
				. '</button><button type="button" class="btn topic-digest-worker-restart" '
				. 'data-topic-digest-restart="1" data-success="'
				. $esc(_t('ext.topic_digest.restart_requested')) . '">'
				. $esc(_t('ext.topic_digest.restart_worker'))
				. '</button><span class="topic-digest-control-status" role="status" aria-live="polite" '
				. 'data-topic-digest-control-status="1"></span></div></details>';
		} catch (Throwable $e) {
			Minz_Log::error('Topic Digest statistics error: ' . $e->getMessage());
			return '';
		}
	}

	/**
	 * @return array{summary:string,items:array<string,array{label:string,value:string}>,
	 *     control:array{action:string,label:string},topic_counts:array<int,int>}
	 */
	private function formattedStatistics(): array {
		$status = $this->store()->status();
		$status['backfill_remaining'] = $this->backfillRemainingCount();
		$paused = (int)$status['paused'] !== 0;
		return [
			'summary' => _t('ext.topic_digest.stats_summary', (int)$status['queued'], (int)$status['processed']),
			'items' => [
				'queued' => ['label' => _t('ext.topic_digest.queued'), 'value' => (string)(int)$status['queued']],
				'processing' => ['label' => _t('ext.topic_digest.processing'),
					'value' => (string)(int)$status['processing']],
				'processed' => ['label' => _t('ext.topic_digest.processed'),
					'value' => (string)(int)$status['processed']],
				'events' => ['label' => _t('ext.topic_digest.events'), 'value' => (string)(int)$status['events']],
				'sources' => ['label' => _t('ext.topic_digest.sources'), 'value' => (string)(int)$status['sources']],
				'last_hour_average_speed' => ['label' => _t('ext.topic_digest.last_hour_average_speed'),
					'value' => (int)$status['last_hour_average_ready'] !== 0
						? _t('ext.topic_digest.articles_per_hour',
							number_format((float)$status['last_hour_average_per_hour'], 1))
						: _t('ext.topic_digest.estimate_calculating')],
				'all_time_average_speed' => ['label' => _t('ext.topic_digest.all_time_average_speed'),
					'value' => (int)$status['average_ready'] !== 0
						? _t('ext.topic_digest.articles_per_hour', number_format((float)$status['average_per_hour'], 1))
						: _t('ext.topic_digest.estimate_calculating')],
				'estimated_time' => ['label' => _t('ext.topic_digest.estimated_time'),
					'value' => $this->estimatedTime($status)],
				'failed' => ['label' => _t('ext.topic_digest.failed'), 'value' => (string)(int)$status['failed']],
			],
			'control' => ['action' => $paused ? 'resume' : 'pause',
				'label' => _t('ext.topic_digest.' . ($paused ? 'resume_worker' : 'pause_worker'))],
			'topic_counts' => $this->topicFeedCounts(),
		];
	}

	/** @param array<string,int|float|string> $status */
	public function estimatedTime(array $status): string {
		if ((int)($status['paused'] ?? 0) !== 0) {
			return _t('ext.topic_digest.estimate_paused');
		}
		$remaining = TopicDigestProcessor::estimatedRemainingArticles($status);
		if ($remaining === 0 && (int)($status['backfill_active'] ?? 0) === 0) {
			return _t('ext.topic_digest.estimate_complete');
		}
		$rate = (int)($status['last_hour_average_ready'] ?? 0) !== 0
			? (float)($status['last_hour_average_per_hour'] ?? 0.0)
			: (float)($status['average_per_hour'] ?? 0.0);
		if ($remaining === null || $rate <= 0.0) {
			return _t('ext.topic_digest.estimate_calculating');
		}
		$minutes = max(1, (int)ceil(($remaining / $rate) * 60));
		$days = intdiv($minutes, 1440);
		$hours = intdiv($minutes % 1440, 60);
		$remainingMinutes = $minutes % 60;
		$duration = $days > 0 ? "{$days}d {$hours}h"
			: ($hours > 0 ? "{$hours}h {$remainingMinutes}min" : "{$remainingMinutes}min");
		return _t('ext.topic_digest.estimated_duration', $duration);
	}

	private function backfillRemainingCount(): int {
		$backfill = $this->store()->backfill();
		if (!$backfill['active']) {
			return 0;
		}
		$entryDao = FreshRSS_Factory::createEntryDao();
		if ($backfill['cursor'] === '99999999999999999999') {
			return $entryDao->count();
		}
		$count = $entryDao->fetchInt(
			'SELECT COUNT(*) FROM `_entry` WHERE id <= :id_max',
			[':id_max' => $backfill['cursor']]
		);
		return $count ?? -1;
	}

	#[\Override]
	public function handleConfigureAction(): void {
		parent::handleConfigureAction();
		$this->registerTranslates();
		if (!Minz_Request::isPost() && Minz_Request::paramString('topic_digest_action') === 'stats') {
			header('Cache-Control: no-store');
			$this->sendJsonPayload($this->formattedStatistics());
		}
		if (Minz_Request::isPost()) {
			$this->handlePost(Minz_Request::paramString('topic_digest_action'));
		}
		foreach ($this->store()->topics() as $topic) {
			$topicLock = $this->acquireTopicLock((int)$topic['id']);
			try {
				$this->synchroniseTopic((int)$topic['id'], false);
			} catch (Throwable $e) {
				Minz_Log::error('Topic Digest reconciliation error: ' . $e->getMessage());
			} finally {
				flock($topicLock, LOCK_UN);
				fclose($topicLock);
			}
		}
		$this->settings = $this->configuration();
		$this->topics = $this->store()->topics();
		foreach ($this->topics as &$topic) {
			$topic['suggestions'] = $this->store()->suggestions((int)$topic['id']);
		}
		$editId = Minz_Request::paramInt('edit_topic');
		$this->editTopic = $editId > 0 ? $this->store()->topic($editId) : null;
		$this->categories = FreshRSS_Factory::createCategoryDao()->listCategories(prePopulateFeeds: false);
		$this->feeds = array_values(array_filter(FreshRSS_Factory::createFeedDao()->listFeeds(),
			fn(FreshRSS_Feed $feed): bool => !$this->isSyntheticFeed($feed->id())));
		$this->status = $this->store()->status();
		$this->errors = $this->store()->recentErrors();
		$this->launchAutomaticWorker();
	}

	private function handlePost(string $action): void {
		if ($action === 'pause' || $action === 'resume') {
			$this->store()->setPaused($action === 'pause');
			if ($action === 'resume') {
				$this->workerLaunchAttempted = false;
				$this->launchAutomaticWorker();
			}
			if (Minz_Request::paramBoolean('_topic_digest_ajax')) {
				$this->sendJsonPayload($this->formattedStatistics());
			}
		} elseif ($action === 'save_settings') {
			$previousConfig = $this->configuration();
			$url = rtrim(Minz_Request::paramString('ollama_url', plaintext: true), '/');
			$profile = Minz_Request::paramString('model_profile', plaintext: true);
			$modelFields = ['local_summary_model', 'local_judge_model', 'local_embedding_model',
				'cloud_summary_model', 'cloud_judge_model', 'cloud_embedding_model'];
			$models = [];
			foreach ($modelFields as $field) {
				$models[$field] = trim(Minz_Request::paramString($field, plaintext: true));
			}
			if (!$this->validOllamaUrl($url) || !in_array($profile, ['local', 'cloud'], true)
					|| count(array_filter($models, [$this, 'validModelName'])) !== count($models)) {
				Minz_Request::bad('Invalid Ollama URL or model name.');
			}
			$prefix = $profile . '_';
			/** @phpstan-ignore method.deprecated */
			$this->setUserConfiguration(['ollama_url' => $url, 'model_profile' => $profile,
				...$models,
				'summary_model' => $models[$prefix . 'summary_model'],
				'judge_model' => $models[$prefix . 'judge_model'],
				'embedding_model' => $models[$prefix . 'embedding_model'],
				'timeout' => min(self::MAX_OLLAMA_TIMEOUT, max(10, Minz_Request::paramInt('timeout'))),
				'scraping' => Minz_Request::paramBoolean('scraping'),
				'always_show_topics' => $previousConfig['always_show_topics'],
				'cloud_concurrency' => $this->validCloudConcurrency(
					Minz_Request::paramString('cloud_concurrency', plaintext: true))]);
			if (!hash_equals($previousConfig['embedding_model'], $models[$prefix . 'embedding_model'])) {
				$this->store()->invalidateEmbeddings();
			}
			$this->store()->startBackfill();
			Minz_Request::good(_t('feedback.conf.updated'), $this->settingsRedirect());
		} elseif ($action === 'switch_profile') {
			$previousConfig = $this->configuration();
			$profile = Minz_Request::paramString('model_profile', plaintext: true);
			if (!in_array($profile, ['local', 'cloud'], true)) {
				Minz_Request::bad('Invalid model profile.');
			}
			$previousConfig['model_profile'] = $profile;
			$previousConfig['summary_model'] = $previousConfig[$profile . '_summary_model'];
			$previousConfig['judge_model'] = $previousConfig[$profile . '_judge_model'];
			$previousConfig['embedding_model'] = $previousConfig[$profile . '_embedding_model'];
			/** @phpstan-ignore method.deprecated */
			$this->setUserConfiguration($previousConfig);
			if (!hash_equals($this->configuration()['embedding_model'], $this->configuration()[$profile . '_embedding_model'])) {
				$this->store()->invalidateEmbeddings();
			}
			$this->store()->requestWorkerRestart();
			$this->store()->startBackfill();
			$this->workerLaunchAttempted = false;
			$this->launchAutomaticWorker();
			Minz_Request::good(_t('ext.topic_digest.profile_switched',
				_t('ext.topic_digest.profile_' . $profile)), $this->settingsRedirect());
		} elseif ($action === 'save_display_settings') {
			$config = $this->configuration();
			$config['always_show_topics'] = Minz_Request::paramBoolean('always_show_topics');
			/** @phpstan-ignore method.deprecated */
			$this->setUserConfiguration($config);
			Minz_Request::good(_t('feedback.conf.updated'), $this->settingsRedirect());
		} elseif ($action === 'save_topic') {
			$id = Minz_Request::paramInt('topic_id');
			$topicLock = $id > 0 ? $this->acquireTopicLock($id) : null;
			$values = [
				'name' => Minz_Request::paramString('name', plaintext: true),
				'description' => Minz_Request::paramString('description', plaintext: true),
				'exclusions' => preg_split('/\R+/', Minz_Request::paramString('exclusions', plaintext: true), -1, PREG_SPLIT_NO_EMPTY) ?: [],
				'enabled' => Minz_Request::paramBoolean('enabled'), 'confidence' => Minz_Request::paramString('confidence', plaintext: true),
				'all_feeds' => Minz_Request::paramBoolean('all_feeds'), 'all_categories' => Minz_Request::paramBoolean('all_categories'),
				'feed_ids' => Minz_Request::paramArrayInt('feed_ids'), 'category_ids' => Minz_Request::paramArrayInt('category_ids'),
				'backfill_mode' => Minz_Request::paramString('backfill_mode', plaintext: true),
				'backfill_days' => Minz_Request::paramInt('backfill_days'),
				'topic_type' => Minz_Request::paramString('topic_type', plaintext: true),
				'show_verification' => Minz_Request::paramBoolean('show_verification'),
			];
			try {
				$topicId = $this->store()->saveTopic($values, $id > 0 ? $id : null);
				$topicLock ??= $this->acquireTopicLock($topicId);
				$this->store()->startBackfill();
				$this->syntheticFeedIds = null;
				$this->synchroniseTopic($topicId, false);
			} finally {
				if (is_resource($topicLock)) {
					flock($topicLock, LOCK_UN);
					fclose($topicLock);
				}
			}
			Minz_Request::good(_t('feedback.conf.updated'), $this->settingsRedirect());
		} elseif ($action === 'toggle') {
			$topicId = Minz_Request::paramInt('topic_id');
			$topicLock = $this->acquireTopicLock($topicId);
			$enabled = Minz_Request::paramBoolean('enabled');
			try {
				$this->store()->setTopicEnabled($topicId, $enabled);
				if ($enabled) {
					$this->store()->startBackfill();
				}
			} finally {
				flock($topicLock, LOCK_UN);
				fclose($topicLock);
			}
		} elseif ($action === 'rescan') {
			$this->store()->startBackfill();
		} elseif ($action === 'retry') {
			$this->store()->retryFailed();
		} elseif ($action === 'restart') {
			$this->store()->setPaused(true);
			try {
				$this->store()->requestWorkerRestart();
				foreach ($this->store()->topics() as $topic) {
					$topicLock = $this->acquireTopicLock((int)$topic['id']);
					flock($topicLock, LOCK_UN);
					fclose($topicLock);
				}
				$this->store()->rebuildDigests();
				$this->store()->prepareRebuildJobs($this->pipelineHash());
				foreach ($this->store()->topics() as $topic) {
					$topicLock = $this->acquireTopicLock((int)$topic['id']);
					try {
						$this->synchroniseTopic((int)$topic['id'], false, true);
					} finally {
						flock($topicLock, LOCK_UN);
						fclose($topicLock);
					}
				}
			} finally {
				$this->store()->setPaused(false);
			}
			$this->workerLaunchAttempted = false;
			$this->launchAutomaticWorker();
			if (Minz_Request::paramBoolean('_topic_digest_ajax')) {
				$this->sendJsonPayload($this->formattedStatistics());
			}
		} elseif ($action === 'reset_log') {
			$this->store()->clearErrors();
			$this->resetWorkerLog();
		} elseif ($action === 'preview') {
			$topic = $this->store()->topic(Minz_Request::paramInt('topic_id'));
			if ($topic === null) {
				throw new InvalidArgumentException('Unknown topic.');
			}
			$results = (new TopicDigestProcessor($this))->previewTopic($topic, 10);
			$matches = array_values(array_filter($results, static fn(array $result): bool => $result['matches']));
			$names = array_map(static fn(array $result): string => $result['title'] . ' ('
				. number_format($result['confidence'], 2) . ')', $matches);
			Minz_Request::good($matches === [] ? 'No recent articles matched this topic.'
				: 'Recent matches: ' . implode('; ', $names));
		} elseif ($action === 'suggestion') {
			$topicId = Minz_Request::paramInt('topic_id');
			$topicLock = $this->acquireTopicLock($topicId);
			try {
				$this->store()->resolveSuggestion($topicId, Minz_Request::paramInt('suggestion_id'),
					Minz_Request::paramString('decision') === 'approve');
				$this->synchroniseTopic($topicId, false);
			} finally {
				flock($topicLock, LOCK_UN);
				fclose($topicLock);
			}
		} elseif ($action === 'delete' || $action === 'restore_delete') {
			$topicId = Minz_Request::paramInt('topic_id');
			$topicLock = $this->acquireTopicLock($topicId);
			try {
				$topic = $this->store()->topic($topicId);
				if ($topic === null) {
					throw new InvalidArgumentException('Unknown topic.');
				}
				if ($action === 'restore_delete') {
					$ids = [];
					foreach ($this->store()->events($topicId) as $event) {
						$ids = [...$ids, ...$this->store()->restoreEvent($topicId, (int)$event['id'])];
					}
					$this->markUnread(array_values(array_unique($ids)));
				}
				if ((int)($topic['feed_id'] ?? 0) > 0
						&& FreshRSS_Factory::createFeedDao()->searchById((int)$topic['feed_id']) !== null
						&& !FreshRSS_feed_Controller::deleteFeed((int)$topic['feed_id'])) {
					throw new RuntimeException('Could not delete the synthetic Topic Digest feed.');
				}
				$this->store()->deleteTopic($topicId);
				$this->syntheticFeedIds = null;
			} finally {
				flock($topicLock, LOCK_UN);
				fclose($topicLock);
			}
		} elseif ($action === 'test') {
			$config = $this->configuration();
			(new TopicDigestOllama($config['ollama_url'], $config['timeout']))->test([
				$config['summary_model'], $config['judge_model'], $config['embedding_model'],
			]);
			Minz_Request::good('Ollama connection and models are available.');
		} else {
			throw new InvalidArgumentException('Unknown Topic Digest action.');
		}
		Minz_Request::good('Topic Digest updated.');
	}

	/** @return array{c:string,a:string,params:array{e:string}} */
	private function settingsRedirect(): array {
		return ['c' => 'extension', 'a' => 'configure', 'params' => ['e' => $this->getName()]];
	}

	private function validOllamaUrl(string $url): bool {
		$parts = parse_url($url);
		return is_array($parts) && in_array(strtolower((string)($parts['scheme'] ?? '')), ['http', 'https'], true)
			&& !empty($parts['host']) && !isset($parts['user']) && !isset($parts['pass'])
			&& !isset($parts['query']) && !isset($parts['fragment']);
	}

	private function resetWorkerLog(): void {
		$path = $this->getExtensionUserPath() . '/worker.log';
		if (is_file($path) && file_put_contents($path, '', LOCK_EX) === false) {
			throw new RuntimeException('Could not reset the Topic Digest worker log.');
		}
	}

	private function validModelName(string $model): bool {
		return preg_match('/^[A-Za-z0-9][A-Za-z0-9._\/:@-]{0,199}$/D', trim($model)) === 1;
	}

	private function validCloudConcurrency(string $value): string {
		return in_array($value, ['auto', '1', '2', '4', '8', '16'], true) ? $value : 'auto';
	}

	public function launchAutomaticWorker(): void {
		if ($this->workerLaunchAttempted || defined('TOPIC_DIGEST_WORKER') || PHP_OS_FAMILY === 'Windows') {
			return;
		}
		$status = $this->store()->status();
		if ((int)$status['paused'] !== 0
				|| ((int)$status['queued'] === 0 && (int)$status['backfill_active'] === 0)) {
			return;
		}
		$username = Minz_User::name();
		$disabled = array_map('trim', explode(',', (string)ini_get('disable_functions')));
		if ($username === null || $username === '' || !function_exists('exec') || in_array('exec', $disabled, true)) {
			return;
		}
		$this->workerLaunchAttempted = true;
		$php = $this->phpCliBinary();
		if ($php === null) {
			Minz_Log::error('Topic Digest cannot find a PHP CLI executable.');
			return;
		}
		$command = 'nohup ' . escapeshellarg($php) . ' ' . escapeshellarg(__DIR__ . '/cli/daemon.php')
			. ' --user ' . escapeshellarg($username) . ' >> ' . escapeshellarg($this->getExtensionUserPath() . '/worker.log')
			. ' 2>&1 < /dev/null &';
		$output = [];
		$result = 0;
		exec($command, $output, $result);
		if ($result !== 0) {
			Minz_Log::error("Topic Digest automatic worker launch failed with status {$result}.");
		}
	}

	private function phpCliBinary(): ?string {
		foreach (array_unique([PHP_BINDIR . '/php', '/usr/local/bin/php', '/usr/bin/php', PHP_BINARY]) as $candidate) {
			$name = strtolower(basename($candidate));
			if (is_file($candidate) && is_executable($candidate) && !str_contains($name, 'fpm') && !str_contains($name, 'cgi')) {
				return $candidate;
			}
		}
		return null;
	}

	/** @param array<string,mixed> $payload */
	private function sendJsonPayload(array $payload): never {
		header('Content-Type: application/json; charset=UTF-8');
		echo json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR);
		exit;
	}
}
