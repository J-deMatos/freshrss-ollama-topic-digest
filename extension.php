<?php
declare(strict_types=1);

require_once __DIR__ . '/TopicDigestStore.php';
require_once __DIR__ . '/ArticleSummaryCache.php';
require_once __DIR__ . '/TopicDigestOllama.php';
require_once __DIR__ . '/TopicDigestScraper.php';
require_once __DIR__ . '/TopicDigestProcessor.php';

/**
 * @phpstan-type TopicDigestConfig array{ollama_profile:string,effective_ollama_profile:string,ollama_url:string,
 *  summary_model:string,judge_model:string,embedding_model:string,structuring_model:string,timeout:int,scraping:bool,
 *  always_show_topics:bool}
 */
final class TopicDigestExtension extends Minz_Extension {
	private const CATEGORY_NAME = 'Topic Digests';
	private const DEFAULT_OLLAMA_TIMEOUT = 1800;
	private const LEGACY_OLLAMA_TIMEOUT = 180;
	private const MAX_OLLAMA_TIMEOUT = 7200;
	private const OLLAMA_PROFILES = ['local', 'cloud'];
	/** @var array<string,string> */
	private const OLLAMA_PROFILE_DEFAULTS = ['ollama_url' => 'http://ollama:11434', 'summary_model' => 'qwen3.5:4b',
		'judge_model' => 'qwen3.5:9b', 'embedding_model' => 'qwen3-embedding:0.6b'];
	private const MAX_WORKER_LOG_BYTES = 131072;
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
	/** @var list<array{reason:string,count:int}> */
	public array $skipReasons = [];
	public string $workerLog = '';
	public bool $workerLogTruncated = false;
	/** @var array<int,int>|null Feed id => category id, memoised per request for enqueueBackfillPage(). */
	private ?array $feedCategoriesCache = null;

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
		// Filters run before EntryBeforeAdd/EntryBeforeUpdate, so this always records a match in time to skip it.
		if (defined(Minz_HookType::class . '::EntryAutoRead')) {
			$this->registerHook(constant(Minz_HookType::class . '::EntryAutoRead'), [$this, 'trackAutoReadByFilter']);
		}
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
		if (Minz_Request::paramString('topic_digest_action') !== 'stats') {
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
		$profile = $this->effectiveOllamaProfile();
		return [
			'ollama_profile' => $this->storedOllamaProfile(),
			'effective_ollama_profile' => $profile,
			'ollama_url' => $this->ollamaProfileValue($profile, 'ollama_url'),
			'summary_model' => $this->ollamaProfileValue($profile, 'summary_model'),
			'judge_model' => $this->ollamaProfileValue($profile, 'judge_model'),
			'embedding_model' => $this->ollamaProfileValue($profile, 'embedding_model'),
			'structuring_model' => $this->structuringModel(),
			'timeout' => $this->ollamaTimeoutConfiguration(),
			'scraping' => $this->getUserConfigurationBool('scraping') ?? true,
			'always_show_topics' => $this->getUserConfigurationBool('always_show_topics') ?? true,
		];
	}

	/** The user's saved preference, as shown/edited on the settings page. */
	private function storedOllamaProfile(): string {
		$profile = $this->getUserConfigurationString('ollama_profile') ?? 'local';
		return in_array($profile, self::OLLAMA_PROFILES, true) ? $profile : 'local';
	}

	/**
	 * The profile actually used for processing: the stored preference, unless it is "cloud" and Ollama Cloud
	 * most recently rejected a request for account/plan reasons (HTTP 402/429), in which case "local" is used
	 * automatically until the cooldown set by TopicDigestProcessor expires.
	 */
	private function effectiveOllamaProfile(): string {
		$stored = $this->storedOllamaProfile();
		if ($stored === 'cloud' && $this->store()->cloudUnavailableUntil() > time()) {
			return 'local';
		}
		return $stored;
	}

	/** Unix timestamp until which processing is automatically using the local profile, or 0 if not in fallback. */
	public function cloudFallbackUntil(): int {
		return $this->storedOllamaProfile() === 'cloud' ? $this->store()->cloudUnavailableUntil() : 0;
	}

	/**
	 * The model used to recover a structured reply, on the local Ollama endpoint, when a primary endpoint
	 * (e.g. Ollama Cloud, which does not support the "format" JSON schema constraint) returns free text instead
	 * of JSON. Defaults to the local profile's own summary model so this works with no extra setup.
	 */
	private function structuringModel(): string {
		$configured = $this->getUserConfigurationString('structuring_model');
		return $configured !== null && $configured !== '' ? $configured : $this->ollamaProfileValue('local', 'summary_model');
	}

	/** The raw, possibly-blank override, for rendering the settings field without locking in the computed default. */
	public function structuringModelOverride(): string {
		return $this->getUserConfigurationString('structuring_model') ?? '';
	}

	/**
	 * Reads a single Ollama field for one profile, falling back to the pre-profile flat setting for
	 * "local" (so existing installs keep working after upgrading) and to the built-in default otherwise.
	 */
	private function ollamaProfileValue(string $profile, string $field): string {
		$value = $this->getUserConfigurationString("{$profile}_{$field}");
		if ($value !== null) {
			return $value;
		}
		if ($profile === 'local') {
			return $this->getUserConfigurationString($field) ?? self::OLLAMA_PROFILE_DEFAULTS[$field];
		}
		return '';
	}

	/** @return array<string,array<string,string>> */
	public function ollamaProfiles(): array {
		$result = [];
		foreach (self::OLLAMA_PROFILES as $profile) {
			foreach (self::OLLAMA_PROFILE_DEFAULTS as $field => $default) {
				$result[$profile][$field] = $this->ollamaProfileValue($profile, $field);
			}
		}
		return $result;
	}

	private function ollamaTimeoutConfiguration(): int {
		$timeout = $this->getUserConfigurationInt('timeout');
		if ($timeout === null || $timeout === self::LEGACY_OLLAMA_TIMEOUT) {
			return self::DEFAULT_OLLAMA_TIMEOUT;
		}
		return min(self::MAX_OLLAMA_TIMEOUT, max(10, $timeout));
	}

	/**
	 * The models the user has actually configured, ignoring any temporary automatic cloud->local fallback.
	 *
	 * pipelineHash()/analysisHash() must be derived from these, never from the *effective* profile: the automatic
	 * fallback flips back and forth on its own every cooldown, and hashing the effective profile made every one of
	 * those flips invalidate the stored hash of every queued job at once. Each job then took the "pipeline changed"
	 * branch, re-queued itself and did no classification at all, so with a backlog larger than one cooldown's worth
	 * of work the queue could never converge: it churned quickly (no Ollama call on that branch) while matching
	 * nothing, and every summary and embedding was recomputed on each flip.
	 *
	 * @return array{summary_model:string,judge_model:string,embedding_model:string}
	 */
	private function storedProfileModels(): array {
		$profile = $this->storedOllamaProfile();
		return [
			'summary_model' => $this->ollamaProfileValue($profile, 'summary_model'),
			'judge_model' => $this->ollamaProfileValue($profile, 'judge_model'),
			'embedding_model' => $this->ollamaProfileValue($profile, 'embedding_model'),
		];
	}

	public function pipelineHash(): string {
		$models = $this->storedProfileModels();
		$scraping = $this->getUserConfigurationBool('scraping') ?? true;
		$topics = array_map(static fn(array $topic): array => [
			$topic['id'], $topic['rule_hash'], $topic['enabled'], $topic['all_feeds'], $topic['all_categories'],
			$topic['feed_ids'], $topic['category_ids'], $topic['backfill_mode'], $topic['backfill_days'], $topic['topic_type'],
			$topic['show_verification'],
		], $this->store()->topics());
		return hash('sha256', json_encode([$models['summary_model'], $models['judge_model'],
			$models['embedding_model'], $scraping, $this->store()->pipelineRevision(), $topics], JSON_THROW_ON_ERROR));
	}

	public function analysisHash(): string {
		$models = $this->storedProfileModels();
		return hash('sha256', json_encode([$models['summary_model'], $models['embedding_model'],
			$this->getUserConfigurationBool('scraping') ?? true], JSON_THROW_ON_ERROR));
	}

	public function lockPath(): string {
		return $this->getExtensionUserPath() . '/worker.lock';
	}

	/** Records a FreshRSS filter's auto-read match so enqueueEntry() can skip queuing that article. */
	public function trackAutoReadByFilter(FreshRSS_Entry $entry, string $why): void {
		if ($why !== 'filter') {
			return;
		}
		try {
			$this->store()->markFilterRead($entry->id());
		} catch (Throwable $e) {
			Minz_Log::error('Topic Digest filter-read tracking error: ' . $e->getMessage());
		}
	}

	/**
	 * Whether a currently-configured "mark as read" filter (global, category, or feed) would match this entry.
	 * Used as a fallback when there is no live-observed record for it (e.g. it was read before this exclusion
	 * existed): re-deriving the answer from the current rules is the only thing FreshRSS's own data allows,
	 * since it does not persist *why* an entry became read.
	 */
	private function wouldMatchReadFilter(FreshRSS_Entry $entry): bool {
		$feed = $entry->feed();
		if ($feed === null) {
			return false;
		}
		foreach (FreshRSS_Context::userConf()->filtersAction('read') as $booleanSearch) {
			if ($entry->matches($booleanSearch)) {
				return true;
			}
		}
		$category = $feed->category();
		if ($category !== null) {
			foreach ($category->filtersAction('read') as $booleanSearch) {
				if ($entry->matches($booleanSearch)) {
					return true;
				}
			}
		}
		foreach ($feed->filtersAction('read') as $booleanSearch) {
			if ($entry->matches($booleanSearch)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Whether this entry is known (live-observed, or re-derived by checking the entry against the feed's,
	 * category's, and global current "mark as read" filters) to have been auto-read by a FreshRSS filter.
	 * Persists a newly-derived match so future checks (including a later reprocessing pass, e.g. from
	 * "Restart and rebuild Topic Digest") skip straight to the fast path.
	 */
	public function isFilterReadEntry(FreshRSS_Entry $entry): bool {
		if (!$entry->isRead()) {
			// An unread entry cannot have been auto-read by a filter. Checking this before trusting the stored
			// marker matters because EntryAutoRead fires with the entry's *provisional* id, which FreshRSS may
			// later assign to a different article: without this, that unrelated article would inherit the marker
			// and be skipped (and un-read) as though a filter had read it.
			return false;
		}
		if ($this->store()->isFilterRead($entry->id())) {
			return true;
		}
		try {
			if ($this->wouldMatchReadFilter($entry)) {
				$this->store()->markFilterRead($entry->id());
				return true;
			}
		} catch (Throwable $e) {
			Minz_Log::error('Topic Digest read-filter recheck error: ' . $e->getMessage());
		}
		return false;
	}

	public function enqueueEntry(FreshRSS_Entry $entry): FreshRSS_Entry {
		if ($this->isSyntheticFeed($entry->feedId()) || $this->store()->topics(true) === []
				|| $this->isFilterReadEntry($entry)) {
			return $entry;
		}
		try {
			$this->store()->enqueue($entry, $entry->feed()?->categoryId() ?? 0, $this->pipelineHash(), 100, 30);
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
		// Memoised per request: this is called in a tight loop (the whole backfill is now enqueued up front),
		// and the feed list rarely changes mid-scan.
		if ($this->feedCategoriesCache === null) {
			$this->feedCategoriesCache = [];
			foreach (FreshRSS_Factory::createFeedDao()->selectAll() as $feed) {
				$this->feedCategoriesCache[(int)$feed['id']] = (int)$feed['category'];
			}
		}
		$feedCategories = $this->feedCategoriesCache;
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
			$seenAt = $this->generatedEntrySeenAt($feedId, $prune);
			$this->synchroniseHighPriorityFeed($topic, $feedId, $prune, $seenAt);
			$overview = $this->synchroniseOverviewEntry(
				$topic, $feedId, $markUnread, $preservedOverview, true, $seenAt
			);
			if ($prune) {
				// Only after every entry that should survive has been (re-)written with $seenAt.
				$this->pruneGeneratedEntries($feedId);
			}
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

	/**
	 * Only the source that opened an event is materialised as its own entry; further sources joining the same
	 * event are still recorded in "sources" (for "why matched"/Restore and the pinned overview's source list)
	 * but do not get a further entry of their own — otherwise the same real-world story, covered by several
	 * matched articles, showed up as that many separate unread entries in the high-priority feed.
	 *
	 * @param array<string,mixed> $topic
	 */
	private function synchroniseHighPriorityFeed(array $topic, int $feedId, bool $prune, int $seenAt): void {
		$entryDao = FreshRSS_Factory::createEntryDao();
		$primarySourceIds = [];
		foreach ($this->store()->events((int)$topic['id']) as $event) {
			$primaryId = self::primarySourceEntryId($event);
			if ($primaryId !== null) {
				$primarySourceIds[] = $primaryId;
			}
		}
		$existing = [];
		if ($prune) {
			foreach ($entryDao->listWhere(type: 'f', id: $feedId, state: FreshRSS_Entry::STATE_ALL, limit: -1) as $entry) {
				$existing[$entry->guid()] = $entry;
			}
		}
		foreach (array_values(array_unique($primarySourceIds)) as $sourceId) {
			$guid = $this->topicSourceGuid((int)$topic['id'], $sourceId);
			$this->materialiseHighPriorityEntry($topic, $feedId, $sourceId, $seenAt, $existing[$guid] ?? null);
		}
	}

	/**
	 * The entry_id of the source that opened this event, i.e. the one materialised as its own entry in a
	 * high-priority feed. Falls back to the earliest-added source for an event stored before primary_entry_id
	 * existed, or when its recorded primary is no longer a member (see TopicDigestStore::removeEmptyEvent()).
	 *
	 * @param array<string,mixed> $event
	 */
	private static function primarySourceEntryId(array $event): ?string {
		$primary = (string)($event['primary_entry_id'] ?? '');
		/** @var list<array<string,mixed>> $sources */
		$sources = $event['sources'];
		foreach ($sources as $source) {
			if ($primary !== '' && (string)$source['entry_id'] === $primary) {
				return $primary;
			}
		}
		if ($sources === []) {
			return null;
		}
		usort($sources, static fn(array $left, array $right): int => $left['added_at'] <=> $right['added_at']);
		return (string)$sources[0]['entry_id'];
	}

	/**
	 * A lastSeen value strictly newer than every entry currently in the feed. Stamping it on each entry that
	 * should survive is what makes pruneGeneratedEntries() below delete exactly the rest.
	 */
	private function generatedEntrySeenAt(int $feedId, bool $prune): int {
		$now = time();
		if (!$prune) {
			return $now;
		}
		$newest = 0;
		foreach (FreshRSS_Factory::createEntryDao()->listWhere(type: 'f', id: $feedId,
				state: FreshRSS_Entry::STATE_ALL, limit: -1) as $entry) {
			$newest = max($newest, $entry->lastSeen());
		}
		return max($now, $newest + 1);
	}

	/**
	 * Removes the generated articles this synchronisation did not just re-write, i.e. those whose topic
	 * membership is gone — most of them at once after a rebuild clears every membership.
	 *
	 * FreshRSS's only entry-deletion API is cleanOldEntries(), which always keeps everything carrying the feed's
	 * highest lastSeen (its "seen at the last refresh" guard) and, called with no options, also appends a literal
	 * "AND (1=0)" and so deletes nothing whatsoever. That is why this pruning silently never removed anything,
	 * leaving high-priority feeds full of unread articles belonging to topics with no matches left. Re-writing
	 * every surviving entry with one fresh lastSeen first turns that guard into precisely the rule needed here.
	 * Favourites are kept, like everywhere else in this extension.
	 */
	private function pruneGeneratedEntries(int $feedId): void {
		$removed = FreshRSS_Factory::createEntryDao()->cleanOldEntries($feedId, [
			'keep_period' => 'PT0S',
			'keep_favourites' => true,
		]);
		if ($removed === false) {
			throw new RuntimeException('Could not remove stale articles from the high-priority Topic Digest feed.');
		}
	}

	/**
	 * $isNewEvent also gates whether $sourceEntryId gets its own entry in the high-priority feed: only the
	 * source that opens a new event does. A further source joining an already-materialised event is still
	 * recorded in "sources" (surfaced via the pinned overview and "why matched"/Restore) but does not spawn a
	 * second entry for what the reader would see as the same story.
	 */
	public function materialiseTopicSource(int $topicId, string $sourceEntryId, bool $isNewEvent): void {
		$topic = $this->store()->topic($topicId);
		if ($topic === null || $topic['topic_type'] !== 'feed') {
			return;
		}
		$feedId = (int)($topic['feed_id'] ?? 0);
		if ($feedId < 1 || FreshRSS_Factory::createFeedDao()->searchById($feedId) === null) {
			$this->synchroniseTopic($topicId, false);
			return;
		}
		$seenAt = time();
		if ($isNewEvent) {
			$this->materialiseHighPriorityEntry($topic, $feedId, $sourceEntryId, $seenAt);
		}
		$overview = $this->synchroniseOverviewEntry($topic, $feedId, $isNewEvent, pinned: true, seenAt: $seenAt);
		$this->store()->attachSynthetic($topicId, $feedId, $overview->id());
		FreshRSS_Factory::createFeedDao()->updateCachedValues($feedId);
		FreshRSS_UserDAO::touch();
	}

	/** @param array<string,mixed> $topic */
	private function materialiseHighPriorityEntry(array $topic, int $feedId, string $sourceEntryId, int $seenAt,
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
		// Marks this entry as still wanted, so a following pruneGeneratedEntries() spares it. Without it the
		// value copied from the source article (or from the previous copy) leaves it looking stale.
		$values['lastSeen'] = $seenAt;
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
		?FreshRSS_Entry $detached = null, bool $pinned = false, ?int $seenAt = null): FreshRSS_Entry {
		$seenAt ??= time();
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
			$entry->_lastSeen($seenAt);
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
		// The overview must never look stale to pruneGeneratedEntries(); it is the one entry that always survives.
		$values['lastSeen'] = $seenAt;
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
		$html = '<form class="topic-digest-action" method="post" action="' . $url . '">'
			. '<input type="hidden" name="_csrf" value="' . htmlspecialchars(FreshRSS_Auth::csrfToken(), ENT_QUOTES, 'UTF-8') . '" />'
			. '<input type="hidden" name="topic_id" value="' . $topicId . '" />';
		foreach ($parameters as $key => $value) {
			$html .= '<input type="hidden" name="' . htmlspecialchars($key, ENT_QUOTES, 'UTF-8') . '" value="'
				. htmlspecialchars($value, ENT_QUOTES, 'UTF-8') . '" />';
		}
		return $html . '<button type="submit" class="btn btn-mini">' . htmlspecialchars($label, ENT_QUOTES, 'UTF-8')
			. '</button></form>';
	}

	/** Returns the URL only when it is a plain http(s) link safe to render as an href, otherwise null. */
	public function safeUrl(string $url): ?string {
		$parts = parse_url($url);
		return is_array($parts) && in_array(strtolower((string)($parts['scheme'] ?? '')), ['http', 'https'], true)
			&& !empty($parts['host']) && !isset($parts['user']) && !isset($parts['pass']) ? $url : null;
	}

	/** @return list<string> */
	public function restoreSource(int $topicId, string $entryId): array {
		$ids = $this->store()->restoreSource($topicId, $entryId);
		$this->markUnread($ids);
		$this->synchroniseTopic($topicId, false, true);
		return $ids;
	}

	/** @return list<string> */
	public function restoreEvent(int $topicId, int $eventId): array {
		$ids = $this->store()->restoreEvent($topicId, $eventId);
		$this->markUnread($ids);
		$this->synchroniseTopic($topicId, false, true);
		return $ids;
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
				'average_speed' => ['label' => _t('ext.topic_digest.average_speed'),
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
		$queued = (int)($status['queued'] ?? 0);
		$backfilling = (int)($status['backfill_active'] ?? 0) !== 0;
		if ($queued === 0) {
			return $backfilling ? _t('ext.topic_digest.estimate_calculating') : _t('ext.topic_digest.estimate_complete');
		}
		$rate = (float)($status['average_per_hour'] ?? 0.0);
		if ($rate <= 0.0) {
			return _t('ext.topic_digest.estimate_calculating');
		}
		$minutes = max(1, (int)ceil(($queued / $rate) * 60));
		$days = intdiv($minutes, 1440);
		$hours = intdiv($minutes % 1440, 60);
		$remainingMinutes = $minutes % 60;
		$duration = $days > 0 ? "{$days}d {$hours}h"
			: ($hours > 0 ? "{$hours}h {$remainingMinutes}min" : "{$remainingMinutes}min");
		return $backfilling
			? _t('ext.topic_digest.estimated_duration_backfilling', $duration)
			: _t('ext.topic_digest.estimated_duration', $duration);
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
		// Only re-synchronise topics whose FreshRSS objects are actually missing or stale. Doing it unconditionally
		// rewrote every synthetic feed, its overview entry, and every materialised article on every settings page
		// load, which is a heavy write burst competing with the worker for the SQLite write lock.
		$this->reconcileMissingTopics();
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
		$this->skipReasons = $this->store()->skipReasons();
		$this->loadWorkerLog();
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
			$profile = Minz_Request::paramString('ollama_profile', plaintext: true);
			if (!in_array($profile, self::OLLAMA_PROFILES, true)) {
				Minz_Request::bad('Invalid Ollama profile.');
			}
			$values = ['ollama_profile' => $profile];
			foreach (self::OLLAMA_PROFILES as $key) {
				$url = rtrim(Minz_Request::paramString("{$key}_ollama_url", plaintext: true), '/');
				$models = [Minz_Request::paramString("{$key}_summary_model", plaintext: true),
					Minz_Request::paramString("{$key}_judge_model", plaintext: true),
					Minz_Request::paramString("{$key}_embedding_model", plaintext: true)];
				$blank = $url === '' && count(array_filter($models, static fn(string $m): bool => trim($m) !== '')) === 0;
				if (($key === $profile || !$blank)
						&& (!$this->validOllamaUrl($url) || count(array_filter($models, [$this, 'validModelName'])) !== 3)) {
					Minz_Request::bad("Invalid Ollama URL or model name for the {$key} profile.");
				}
				$values["{$key}_ollama_url"] = $url;
				$values["{$key}_summary_model"] = trim($models[0]);
				$values["{$key}_judge_model"] = trim($models[1]);
				$values["{$key}_embedding_model"] = trim($models[2]);
			}
			$structuringModel = trim(Minz_Request::paramString('structuring_model', plaintext: true));
			if ($structuringModel !== '' && !$this->validModelName($structuringModel)) {
				Minz_Request::bad('Invalid structuring model name.');
			}
			/** @phpstan-ignore method.deprecated */
			$this->setUserConfiguration([...$values, 'structuring_model' => $structuringModel,
				'timeout' => min(self::MAX_OLLAMA_TIMEOUT, max(10, Minz_Request::paramInt('timeout'))),
				'scraping' => Minz_Request::paramBoolean('scraping'),
				'always_show_topics' => $previousConfig['always_show_topics']]);
			if (!hash_equals($previousConfig['embedding_model'], $values["{$profile}_embedding_model"])) {
				$this->store()->invalidateEmbeddings();
			}
			$this->store()->startBackfill();
			Minz_Request::good(_t('feedback.conf.updated'), $this->settingsRedirect());
		} elseif ($action === 'save_display_settings') {
			$this->setUserConfigurationValue('always_show_topics', Minz_Request::paramBoolean('always_show_topics'));
			Minz_Request::good(_t('feedback.conf.updated'), $this->settingsRedirect());
		} elseif ($action === 'save_topic') {
			$id = Minz_Request::paramInt('topic_id');
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
			$topicId = $this->store()->saveTopic($values, $id > 0 ? $id : null);
			$this->store()->startBackfill();
			$this->syntheticFeedIds = null;
			$this->synchroniseTopic($topicId, false);
			Minz_Request::good(_t('feedback.conf.updated'), $this->settingsRedirect());
		} elseif ($action === 'toggle') {
			$enabled = Minz_Request::paramBoolean('enabled');
			$this->store()->setTopicEnabled(Minz_Request::paramInt('topic_id'), $enabled);
			if ($enabled) {
				$this->store()->startBackfill();
			}
		} elseif ($action === 'rescan') {
			$this->store()->startBackfill();
		} elseif ($action === 'retry') {
			$this->store()->retryFailed();
		} elseif ($action === 'retry_skipped') {
			$this->store()->retrySkipped();
			$this->workerLaunchAttempted = false;
			$this->launchAutomaticWorker();
		} elseif ($action === 'restart') {
			// Pausing here is only to keep the worker out of the rebuild; restore whatever the user had chosen
			// rather than silently resuming a worker they had deliberately paused.
			$wasPaused = $this->store()->isPaused();
			$this->store()->setPaused(true);
			try {
				$this->store()->rebuildDigests();
				$this->store()->prepareRebuildJobs($this->pipelineHash());
				foreach ($this->store()->topics() as $topic) {
					$this->synchroniseTopic((int)$topic['id'], false, true);
				}
				$this->store()->requestWorkerRestart();
			} finally {
				$this->store()->setPaused($wasPaused);
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
			$edited = Minz_Request::paramString('suggestion_text', plaintext: true);
			$this->store()->resolveSuggestion($topicId, Minz_Request::paramInt('suggestion_id'),
				Minz_Request::paramString('decision') === 'approve', $edited === '' ? null : $edited);
			$this->synchroniseTopic($topicId, false);
		} elseif ($action === 'delete' || $action === 'restore_delete') {
			$topicId = Minz_Request::paramInt('topic_id');
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
		} elseif ($action === 'test') {
			// Test the saved preference, not any automatic cloud-cooldown fallback, so this always reflects the
			// profile the user is actually looking at.
			$config = $this->configuration();
			$tested = $this->ollamaProfiles()[$config['ollama_profile']];
			(new TopicDigestOllama($tested['ollama_url'], $config['timeout']))->test([
				$tested['summary_model'], $tested['judge_model'], $tested['embedding_model'],
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

	public function maxWorkerLogKilobytes(): int {
		return (int)round(self::MAX_WORKER_LOG_BYTES / 1024);
	}

	private function workerLogPath(): string {
		return $this->getExtensionUserPath() . '/worker.log';
	}

	private function resetWorkerLog(): void {
		$path = $this->workerLogPath();
		if (is_file($path) && file_put_contents($path, '', LOCK_EX) === false) {
			throw new RuntimeException('Could not reset the Topic Digest worker log.');
		}
	}

	private function loadWorkerLog(): void {
		$path = $this->workerLogPath();
		$size = is_file($path) ? (filesize($path) ?: 0) : 0;
		$this->workerLogTruncated = $size > self::MAX_WORKER_LOG_BYTES;
		if ($size === 0) {
			$this->workerLog = '';
			return;
		}
		$handle = fopen($path, 'rb');
		if ($handle === false) {
			$this->workerLog = '';
			return;
		}
		try {
			fseek($handle, max(0, $size - self::MAX_WORKER_LOG_BYTES));
			$content = stream_get_contents($handle);
			$content = $content === false ? '' : $content;
			if ($this->workerLogTruncated) {
				$firstBreak = strpos($content, "\n");
				$content = $firstBreak === false ? $content : substr($content, $firstBreak + 1);
			}
			$this->workerLog = $content;
		} finally {
			fclose($handle);
		}
	}

	private function validModelName(string $model): bool {
		return preg_match('/^[A-Za-z0-9][A-Za-z0-9._\/:@-]{0,199}$/D', trim($model)) === 1;
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
