<?php
declare(strict_types=1);

require_once __DIR__ . '/TopicDigestStore.php';
require_once __DIR__ . '/ArticleSummaryCache.php';
require_once __DIR__ . '/TopicDigestTextProvider.php';
require_once __DIR__ . '/TopicDigestOllama.php';
require_once __DIR__ . '/TopicDigestOpenAICompatible.php';
require_once __DIR__ . '/TopicDigestTextProviderChain.php';
require_once __DIR__ . '/TopicDigestParallelTester.php';
require_once __DIR__ . '/TopicDigestConcurrentDispatcher.php';
require_once __DIR__ . '/TopicDigestScraper.php';
require_once __DIR__ . '/TopicDigestProcessor.php';

/**
 * @phpstan-type TopicDigestConfig array{ollama_profile:string,effective_ollama_profile:string,ollama_url:string,
 *  summary_model:string,judge_model:string,embedding_model:string,structuring_model:string,timeout:int,scraping:bool,
 *  always_show_topics:bool,fallback_text_mode:string,fallback_text_type:?string,fallback_text_url:?string,
 *  fallback_text_summary_model:?string,fallback_text_judge_model:?string,embedding_provider_url:string,
 *  embedding_provider_model:string,openai_base_url:string,openai_summary_model:string,openai_judge_model:string,
 *  worker_concurrency:int}
 */
final class TopicDigestExtension extends Minz_Extension {
	private const CATEGORY_NAME = 'Topic Digests';
	private const DEFAULT_OLLAMA_TIMEOUT = 1800;
	private const LEGACY_OLLAMA_TIMEOUT = 180;
	private const MAX_OLLAMA_TIMEOUT = 7200;
	/**
	 * How many articles' inference may run concurrently. Deliberately conservative: this bounds real,
	 * simultaneous outbound HTTP requests to whatever text/embedding provider is configured, not just a
	 * local resource. Default 1 reproduces the original, fully sequential worker exactly.
	 */
	private const MAX_WORKER_CONCURRENCY = 8;
	private const OLLAMA_PROFILES = ['local', 'cloud'];
	/**
	 * The primary *text* provider: "local"/"cloud" are the original Ollama profiles (unchanged meaning);
	 * "openai_compatible" is new and uses the configured OpenAI-compatible endpoint directly as the
	 * primary, not just as a fallback. Stored under the same "ollama_profile" key as before for full
	 * backward compatibility (an existing "local"/"cloud" value keeps meaning exactly what it always did).
	 */
	private const PRIMARY_PROVIDER_TYPES = ['local', 'cloud', 'openai_compatible'];
	/**
	 * How the fallback *text* provider is chosen once the primary hits an account/plan limit (HTTP 402/429):
	 * "local" is the default and reproduces the original, implicit Ollama Cloud -> local Ollama pairing
	 * byte-for-byte when the primary is "cloud" (generalized to any non-local primary); "openai_compatible"
	 * routes to the configured OpenAI-compatible endpoint instead; "none" disables fallback entirely.
	 */
	private const FALLBACK_TEXT_MODES = ['local', 'openai_compatible', 'none'];
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
		$fallback = $this->fallbackTextIdentity();
		$embedding = $this->embeddingConfiguration();
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
			'fallback_text_mode' => $this->fallbackTextMode(),
			'fallback_text_type' => $fallback['type'] ?? null,
			'fallback_text_url' => $fallback['url'] ?? null,
			'fallback_text_summary_model' => $fallback['summary_model'] ?? null,
			'fallback_text_judge_model' => $fallback['judge_model'] ?? null,
			'embedding_provider_url' => $embedding['url'],
			'embedding_provider_model' => $embedding['model'],
			// The shared OpenAI-compatible field set, usable as the primary and/or the fallback. Never the API key.
			'openai_base_url' => $this->openAiCompatibleConfiguration()['base_url'],
			'openai_summary_model' => $this->openAiCompatibleConfiguration()['summary_model'],
			'openai_judge_model' => $this->openAiCompatibleConfiguration()['judge_model'],
			'worker_concurrency' => $this->workerConcurrency(),
		];
	}

	/** How many articles' inference may run concurrently; 1 (fully sequential, the original behaviour) by default. */
	public function workerConcurrency(): int {
		$concurrency = $this->getUserConfigurationInt('worker_concurrency');
		if ($concurrency === null) {
			return 1;
		}
		return min(self::MAX_WORKER_CONCURRENCY, max(1, $concurrency));
	}

	/**
	 * The user's saved primary-text-provider preference, as shown/edited on the settings page: "local" or
	 * "cloud" (the original two Ollama profiles) or "openai_compatible".
	 */
	private function storedOllamaProfile(): string {
		$profile = $this->getUserConfigurationString('ollama_profile') ?? 'local';
		return in_array($profile, self::PRIMARY_PROVIDER_TYPES, true) ? $profile : 'local';
	}

	/**
	 * The Ollama profile actually used for processing when the primary is Ollama-based: the stored
	 * preference, unless it is "cloud" and Ollama Cloud most recently rejected a request for account/plan
	 * reasons (HTTP 402/429), in which case "local" is used automatically until the cooldown set by
	 * TopicDigestProcessor expires. Meaningless (and unused) while the primary is "openai_compatible".
	 */
	private function effectiveOllamaProfile(): string {
		$stored = $this->storedOllamaProfile();
		if ($stored === 'cloud' && $this->store()->cloudUnavailableUntil() > time()) {
			return 'local';
		}
		return $stored === 'openai_compatible' ? 'local' : $stored;
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

	/** The user's saved fallback-text-provider preference, as shown/edited on the settings page. */
	public function fallbackTextMode(): string {
		$mode = $this->getUserConfigurationString('fallback_text_mode') ?? 'local';
		return in_array($mode, self::FALLBACK_TEXT_MODES, true) ? $mode : 'local';
	}

	/** @return array{base_url:string,api_key:string,summary_model:string,judge_model:string} */
	private function openAiCompatibleConfiguration(): array {
		return [
			'base_url' => rtrim($this->getUserConfigurationString('openai_base_url') ?? '', '/'),
			'api_key' => $this->getUserConfigurationString('openai_api_key') ?? '',
			'summary_model' => trim($this->getUserConfigurationString('openai_summary_model') ?? ''),
			'judge_model' => trim($this->getUserConfigurationString('openai_judge_model') ?? ''),
		];
	}

	/** Whether an OpenAI-compatible API key has already been saved (never the key itself). */
	public function hasOpenAiApiKey(): bool {
		return ($this->getUserConfigurationString('openai_api_key') ?? '') !== '';
	}

	/**
	 * The primary text provider's identity: "local"/"cloud" resolve to that Ollama profile's fields;
	 * "openai_compatible" resolves to the (shared) OpenAI-compatible field set. Kept separate from the
	 * *effective* profile so that an automatic quota-driven fallback never changes it.
	 * @return array{type:string,url:string,summary_model:string,judge_model:string}
	 */
	private function primaryTextIdentity(): array {
		$profile = $this->storedOllamaProfile();
		if ($profile === 'openai_compatible') {
			$config = $this->openAiCompatibleConfiguration();
			return ['type' => 'openai-compatible', 'url' => $config['base_url'],
				'summary_model' => $config['summary_model'], 'judge_model' => $config['judge_model']];
		}
		return ['type' => 'ollama', 'url' => $this->ollamaProfileValue($profile, 'ollama_url'),
			'summary_model' => $this->ollamaProfileValue($profile, 'summary_model'),
			'judge_model' => $this->ollamaProfileValue($profile, 'judge_model')];
	}

	/**
	 * The fallback text provider's identity, excluding its API key, or null while no fallback is
	 * configured/usable. "local" mode (the default) reproduces the exact pre-existing Ollama Cloud ->
	 * local Ollama pairing, generalized to any non-local primary (a "local" primary has nothing to fall
	 * back to, exactly as before this setting existed). Note that "openai_compatible" as both the primary
	 * and the fallback shares one field set, so that combination is a harmless no-op fallback, not two
	 * independent OpenAI-compatible endpoints — a deliberate v1 simplification.
	 * @return array{type:string,url:string,summary_model:string,judge_model:string}|null
	 */
	private function fallbackTextIdentity(): ?array {
		if ($this->fallbackTextMode() === 'local') {
			if ($this->storedOllamaProfile() === 'local') {
				return null;
			}
			return ['type' => 'ollama', 'url' => $this->ollamaProfileValue('local', 'ollama_url'),
				'summary_model' => $this->ollamaProfileValue('local', 'summary_model'),
				'judge_model' => $this->ollamaProfileValue('local', 'judge_model')];
		}
		if ($this->fallbackTextMode() === 'openai_compatible') {
			$config = $this->openAiCompatibleConfiguration();
			if ($config['base_url'] === '' || $config['summary_model'] === '' || $config['judge_model'] === '') {
				return null;
			}
			return ['type' => 'openai-compatible', 'url' => $config['base_url'],
				'summary_model' => $config['summary_model'], 'judge_model' => $config['judge_model']];
		}
		return null;
	}

	/**
	 * The embedding provider, fully decoupled from whichever text provider is in use. Defaults (while the
	 * dedicated setting has never been saved) to whatever this install's *stored* text profile already used
	 * for embeddings before this decoupling existed, so nothing changes for an existing install until it
	 * explicitly edits the new "Embedding provider" settings field.
	 * @return array{url:string,model:string}
	 */
	private function embeddingConfiguration(): array {
		// ollamaProfileValue() only has a built-in default for "local" (and "cloud" only once the user has
		// actually saved cloud fields); a stored primary of "openai_compatible" has no Ollama fields of its own
		// to fall back to at all, so the sensible default source is always "local" in that case.
		$profile = $this->storedOllamaProfile();
		$defaultProfile = $profile === 'openai_compatible' ? 'local' : $profile;
		$url = $this->getUserConfigurationString('embedding_ollama_url');
		$model = $this->getUserConfigurationString('embedding_provider_model');
		return [
			'url' => $url !== null && $url !== '' ? rtrim($url, '/') : $this->ollamaProfileValue($defaultProfile, 'ollama_url'),
			'model' => $model !== null && $model !== '' ? $model : $this->ollamaProfileValue($defaultProfile, 'embedding_model'),
		];
	}

	/** @return array{type:string,url:string,model:string} */
	private function embeddingIdentity(): array {
		$config = $this->embeddingConfiguration();
		return ['type' => 'ollama', 'url' => $config['url'], 'model' => $config['model']];
	}

	/** A stable identity string for TopicDigestStore::lastEmbeddingIdentity()'s change detection. */
	public function embeddingIdentityHash(): string {
		return hash('sha256', json_encode($this->embeddingIdentity(), JSON_THROW_ON_ERROR));
	}

	/**
	 * Whether the primary text provider is currently being skipped in favour of the fallback: the stored
	 * preference is untouched either way (see textProviderChain()/markPrimaryTextUnavailable()).
	 */
	public function primaryTextInCooldown(): bool {
		return match ($this->fallbackTextMode()) {
			'local' => $this->storedOllamaProfile() === 'cloud' && $this->store()->cloudUnavailableUntil() > time(),
			'openai_compatible' => $this->store()->primaryTextFallbackUntil() > time(),
			default => false,
		};
	}

	/** Unix timestamp until which processing is automatically using the fallback text provider, or 0. */
	public function textFallbackUntil(): int {
		if (!$this->primaryTextInCooldown()) {
			return 0;
		}
		return match ($this->fallbackTextMode()) {
			'local' => $this->store()->cloudUnavailableUntil(),
			'openai_compatible' => $this->store()->primaryTextFallbackUntil(),
			default => 0,
		};
	}

	/** Records that the primary text provider just rejected a request for account/plan reasons. */
	public function markPrimaryTextUnavailable(int $untilTimestamp): void {
		match ($this->fallbackTextMode()) {
			'local' => $this->store()->markCloudUnavailable($untilTimestamp),
			'openai_compatible' => $this->store()->markPrimaryTextFallback($untilTimestamp),
			default => null,
		};
	}

	/**
	 * Builds the primary text provider, bound to the *stored* (not effective) profile. $transportSource, when
	 * given, routes requests through it instead of real blocking curl — either the settings-page's
	 * TopicDigestParallelTester (a fast, concurrent "Test connection" check) or the worker's
	 * TopicDigestConcurrentDispatcher (real concurrent inference for one wavefront of claimed jobs).
	 */
	public function buildPrimaryTextProvider(int $timeout, ?TopicDigestTransportSource $transportSource = null): TopicDigestTextProvider {
		$identity = $this->primaryTextIdentity();
		if ($identity['type'] === 'openai-compatible') {
			$config = $this->openAiCompatibleConfiguration();
			return new TopicDigestOpenAICompatible($config['base_url'], $config['api_key'], $timeout,
				$transportSource?->openAiTransport(),
				structuringUrl: $this->ollamaProfileValue('local', 'ollama_url'), structuringModel: $this->structuringModel());
		}
		return new TopicDigestOllama($identity['url'], $timeout, $transportSource?->ollamaTransport($identity['url']),
			structuringUrl: $this->ollamaProfileValue('local', 'ollama_url'), structuringModel: $this->structuringModel());
	}

	/** Builds the fallback text provider, or null while none is configured/usable. Never exposes the API key. */
	public function buildFallbackTextProvider(int $timeout, ?TopicDigestTransportSource $transportSource = null): ?TopicDigestTextProvider {
		if ($this->fallbackTextIdentity() === null) {
			return null;
		}
		if ($this->fallbackTextMode() === 'local') {
			$url = $this->ollamaProfileValue('local', 'ollama_url');
			return new TopicDigestOllama($url, $timeout, $transportSource?->ollamaTransport($url),
				structuringUrl: $url, structuringModel: $this->structuringModel());
		}
		$config = $this->openAiCompatibleConfiguration();
		return new TopicDigestOpenAICompatible($config['base_url'], $config['api_key'], $timeout,
			$transportSource?->openAiTransport(),
			structuringUrl: $this->ollamaProfileValue('local', 'ollama_url'), structuringModel: $this->structuringModel());
	}

	/** Builds the embedding provider: always local-Ollama-compatible, fully decoupled from the text provider(s). */
	public function buildEmbeddingProvider(int $timeout, ?TopicDigestTransportSource $transportSource = null): TopicDigestEmbeddingProvider {
		$config = $this->embeddingConfiguration();
		return new TopicDigestOllama($config['url'], $timeout, $transportSource?->ollamaTransport($config['url']));
	}

	/**
	 * Assembles the primary+fallback text providers into one chain. $transportSource, when given, routes every
	 * provider built here through it (see buildPrimaryTextProvider()) — used to build a wavefront-scoped chain
	 * for TopicDigestProcessor's concurrent prepare() stage; omitted, this builds the real-blocking-curl chain
	 * used everywhere else (unchanged from before concurrency support existed).
	 */
	public function textProviderChain(int $timeout, ?TopicDigestTransportSource $transportSource = null): TopicDigestTextProviderChain {
		$primaryIdentity = $this->primaryTextIdentity();
		$fallbackIdentity = $this->fallbackTextIdentity();
		return new TopicDigestTextProviderChain(
			$this->buildPrimaryTextProvider($timeout, $transportSource), $primaryIdentity['summary_model'], $primaryIdentity['judge_model'],
			$primaryIdentity['type'],
			$this->buildFallbackTextProvider($timeout, $transportSource), $fallbackIdentity['summary_model'] ?? null,
			$fallbackIdentity['judge_model'] ?? null, $fallbackIdentity['type'] ?? null,
			$this->primaryTextInCooldown(),
		);
	}

	private function ollamaTimeoutConfiguration(): int {
		$timeout = $this->getUserConfigurationInt('timeout');
		if ($timeout === null || $timeout === self::LEGACY_OLLAMA_TIMEOUT) {
			return self::DEFAULT_OLLAMA_TIMEOUT;
		}
		return min(self::MAX_OLLAMA_TIMEOUT, max(10, $timeout));
	}

	/**
	 * Whether none of the new multi-provider settings are actually in use (primary still local/cloud,
	 * fallback still at its "local" default, embedding fields never explicitly saved). When true,
	 * pipelineHash()/analysisHash() below reproduce the exact pre-existing hash formula (bare model-name
	 * strings, no type/url wrapper) byte-for-byte.
	 *
	 * This matters because upgrading otherwise changed the hash's *shape* even for installs that never
	 * touched anything new — the richer identity tuples hash to different bytes than the old bare strings
	 * even when the underlying values are identical — which forced a one-time full backlog reclassification
	 * (and, since analysisHash() also changed shape, a full summary/embedding recompute) with no real
	 * configuration change behind it. The moment a user actually saves a new-feature setting, save_settings()
	 * already triggers a backfill unconditionally, so switching formulas exactly then costs nothing extra.
	 */
	private function usesOnlyLegacyProviderConfiguration(): bool {
		return $this->storedOllamaProfile() !== 'openai_compatible'
			&& $this->fallbackTextMode() === 'local'
			&& $this->getUserConfigurationString('embedding_ollama_url') === null
			&& $this->getUserConfigurationString('embedding_provider_model') === null;
	}

	/** @return array{summary_model:string,judge_model:string,embedding_model:string} */
	private function legacyProfileModels(): array {
		$profile = $this->storedOllamaProfile();
		return [
			'summary_model' => $this->ollamaProfileValue($profile, 'summary_model'),
			'judge_model' => $this->ollamaProfileValue($profile, 'judge_model'),
			'embedding_model' => $this->ollamaProfileValue($profile, 'embedding_model'),
		];
	}

	/**
	 * Derived only from the *configured* provider chain (primary identity, fallback identity when enabled,
	 * embedding identity) — never from which one happens to be *effectively* active right now. The automatic
	 * quota-driven fallback flips back and forth on its own every cooldown, and hashing the effective
	 * provider made every one of those flips invalidate the stored hash of every queued job at once. Each job
	 * then took the "pipeline changed" branch, re-queued itself and did no classification at all, so with a
	 * backlog larger than one cooldown's worth of work the queue could never converge: it churned quickly (no
	 * inference call on that branch) while matching nothing, and every summary and embedding was recomputed
	 * on each flip. Editing the primary *or* the fallback's summary/embedding model in settings still changes
	 * this hash (as it always did for the single profile), which is the correct, existing "requeue for
	 * reclassification" trigger — it is only the automatic runtime flip that must never do so.
	 *
	 * The judge model is deliberately excluded: this hash gates rebuildDigests()/prepareRebuildJobs(), which
	 * unmatches every already-matched article and re-queues the whole backlog — a disruptive, visible restart.
	 * Topic/event decisions already have their own cache keyed on judgeModelIdentity() (see
	 * TopicDigestProcessor::topicDecisions()/eventDecisions()), so a judge-model change already gets a fresh
	 * decision wherever one is actually needed, without forcing every prior decision to be redone. Trying a new
	 * judge model against already-matched articles is a deliberate action the user can take with a manual
	 * restart, not something an unrelated settings save should force on them.
	 */
	public function pipelineHash(): string {
		$scraping = $this->getUserConfigurationBool('scraping') ?? true;
		$topics = array_map(static fn(array $topic): array => [
			$topic['id'], $topic['rule_hash'], $topic['enabled'], $topic['all_feeds'], $topic['all_categories'],
			$topic['feed_ids'], $topic['category_ids'], $topic['backfill_mode'], $topic['backfill_days'], $topic['topic_type'],
			$topic['show_verification'],
		], $this->store()->topics());
		if ($this->usesOnlyLegacyProviderConfiguration()) {
			$models = $this->legacyProfileModels();
			return hash('sha256', json_encode([$models['summary_model'],
				$models['embedding_model'], $scraping, $this->store()->pipelineRevision(), $topics], JSON_THROW_ON_ERROR));
		}
		$primary = $this->primaryTextIdentity();
		$fallback = $this->fallbackTextIdentity();
		$embedding = $this->embeddingIdentity();
		return hash('sha256', json_encode([
			['type' => $primary['type'], 'url' => $primary['url'], 'summary_model' => $primary['summary_model']],
			$fallback === null ? null
				: ['type' => $fallback['type'], 'url' => $fallback['url'], 'summary_model' => $fallback['summary_model']],
			$embedding, $scraping, $this->store()->pipelineRevision(), $topics,
		], JSON_THROW_ON_ERROR));
	}

	public function analysisHash(): string {
		if ($this->usesOnlyLegacyProviderConfiguration()) {
			$models = $this->legacyProfileModels();
			return hash('sha256', json_encode([$models['summary_model'], $models['embedding_model'],
				$this->getUserConfigurationBool('scraping') ?? true], JSON_THROW_ON_ERROR));
		}
		$primary = $this->primaryTextIdentity();
		$fallback = $this->fallbackTextIdentity();
		return hash('sha256', json_encode([
			['type' => $primary['type'], 'url' => $primary['url'], 'model' => $primary['summary_model']],
			$fallback === null ? null : ['type' => $fallback['type'], 'url' => $fallback['url'], 'model' => $fallback['summary_model']],
			$this->embeddingIdentity(), $this->getUserConfigurationBool('scraping') ?? true,
		], JSON_THROW_ON_ERROR));
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
		// A page returning fewer than $limit rows is not proof there is nothing older left to scan: FreshRSS
		// prunes/archives old entries over time, so a page can land in a range where many ids in between are
		// gone, making this page short while plenty of real, older entries still exist further down. Treating
		// a short page as "reached the end" (the previous `$count === $limit` check) stopped the scan there
		// for good — nothing re-activates it afterwards except a full startBackfill() reset, which used to
		// happen as an (unrelated, buggy) side effect of every settings save and so masked this by effectively
		// retrying the whole scan from the top on a schedule. Now that saves only reset it when something
		// pipelineHash-relevant actually changed, a short page must keep the scan going by itself. Only a
		// genuinely empty page (handled above) means there is nothing left below the cursor.
		$this->store()->advanceBackfill($cursor, true);
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
			// $suppressManualUnreadTracking is a single instance property shared by every job this extension
			// instance ever processes. Under worker concurrency, several jobs' finalize() calls run one after
			// another (never concurrently with each other), but a *prepare()* call for a different job could
			// still be suspended mid-flight (awaiting a provider response) while this code runs. That is safe
			// only because nothing between setting and clearing this flag ever calls Fiber::suspend() (no HTTP
			// call happens here) — PHP Fibers are cooperative, so this whole block always runs to completion
			// before any other fiber gets a turn. Do not add an HTTP/provider call inside this try block without
			// re-examining that invariant.
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
			$previousEmbeddingIdentity = $this->embeddingIdentityHash();
			$previousPipelineHash = $this->pipelineHash();
			$profile = Minz_Request::paramString('ollama_profile', plaintext: true);
			if (!in_array($profile, self::PRIMARY_PROVIDER_TYPES, true)) {
				Minz_Request::bad('Invalid primary text provider.');
			}
			$fallbackMode = Minz_Request::paramString('fallback_text_mode', plaintext: true);
			if (!in_array($fallbackMode, self::FALLBACK_TEXT_MODES, true)) {
				Minz_Request::bad('Invalid fallback text provider.');
			}
			$values = ['ollama_profile' => $profile, 'fallback_text_mode' => $fallbackMode];
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
			// A blank API key field means "leave the saved key unchanged" (the field is never pre-filled with the
			// real key, so a blank submission cannot mean "the user wants an empty key"); the checkbox is the only
			// way to actually clear it. Never logged, never echoed back, never included in any exception message.
			$existingApiKey = $this->getUserConfigurationString('openai_api_key') ?? '';
			$submittedApiKey = trim(Minz_Request::paramString('openai_api_key', plaintext: true));
			$clearApiKey = Minz_Request::paramBoolean('openai_api_key_clear');
			$apiKey = $clearApiKey ? '' : ($submittedApiKey !== '' ? $submittedApiKey : $existingApiKey);
			$openAiBaseUrl = rtrim(Minz_Request::paramString('openai_base_url', plaintext: true), '/');
			$openAiSummaryModel = trim(Minz_Request::paramString('openai_summary_model', plaintext: true));
			$openAiJudgeModel = trim(Minz_Request::paramString('openai_judge_model', plaintext: true));
			$openAiNeeded = $profile === 'openai_compatible' || $fallbackMode === 'openai_compatible';
			$openAiBlank = $openAiBaseUrl === '' && $openAiSummaryModel === '' && $openAiJudgeModel === '' && $apiKey === '';
			if (($openAiNeeded || !$openAiBlank) && (!$this->validOllamaUrl($openAiBaseUrl)
					|| !$this->validModelName($openAiSummaryModel) || !$this->validModelName($openAiJudgeModel))) {
				Minz_Request::bad('Invalid OpenAI-compatible base URL or model name.');
			}
			if ($openAiNeeded && $apiKey === '') {
				Minz_Request::bad('An OpenAI-compatible API key is required.');
			}
			$values['openai_base_url'] = $openAiBaseUrl;
			$values['openai_api_key'] = $apiKey;
			$values['openai_summary_model'] = $openAiSummaryModel;
			$values['openai_judge_model'] = $openAiJudgeModel;
			$embeddingUrl = rtrim(Minz_Request::paramString('embedding_ollama_url', plaintext: true), '/');
			$embeddingModel = trim(Minz_Request::paramString('embedding_provider_model', plaintext: true));
			if (!$this->validOllamaUrl($embeddingUrl) || !$this->validModelName($embeddingModel)) {
				Minz_Request::bad('Invalid embedding provider URL or model name.');
			}
			$values['embedding_ollama_url'] = $embeddingUrl;
			$values['embedding_provider_model'] = $embeddingModel;
			$structuringModel = trim(Minz_Request::paramString('structuring_model', plaintext: true));
			if ($structuringModel !== '' && !$this->validModelName($structuringModel)) {
				Minz_Request::bad('Invalid structuring model name.');
			}
			/** @phpstan-ignore method.deprecated */
			$this->setUserConfiguration([...$values, 'structuring_model' => $structuringModel,
				'timeout' => min(self::MAX_OLLAMA_TIMEOUT, max(10, Minz_Request::paramInt('timeout'))),
				'worker_concurrency' => min(self::MAX_WORKER_CONCURRENCY, max(1, Minz_Request::paramInt('worker_concurrency'))),
				'scraping' => Minz_Request::paramBoolean('scraping'),
				'always_show_topics' => $previousConfig['always_show_topics']]);
			if ($this->embeddingIdentityHash() !== $previousEmbeddingIdentity) {
				$this->store()->invalidateEmbeddings();
				$this->store()->setLastEmbeddingIdentity($this->embeddingIdentityHash());
			}
			// startBackfill() bumps pipeline_revision, which pipelineHash() itself hashes — calling it
			// unconditionally would change pipelineHash()'s output (and so reset the whole backlog to pending
			// via enqueue()'s stale-hash check) on every save, regardless of whether anything hash-relevant
			// actually changed. This defeated, in particular, judge-model-only changes being excluded from
			// pipelineHash() on purpose: saving with a new judge model but nothing else still triggered a full
			// reclassification through this side effect alone. Comparing pipelineHash() computed before/after
			// (call site already at the top of this action, and read again here before any revision bump can
			// have happened) restricts the backfill to saves that actually change something pipelineHash() cares
			// about — the same pattern News Deduplicator's equivalent save action already used correctly.
			if (!hash_equals($previousPipelineHash, $this->pipelineHash())) {
				$this->store()->startBackfill();
			}
			Minz_Request::good(_t('feedback.conf.updated'), $this->settingsRedirect());
		} elseif ($action === 'save_display_settings') {
			$this->setUserConfigurationValue('always_show_topics', Minz_Request::paramBoolean('always_show_topics'));
			Minz_Request::good(_t('feedback.conf.updated'), $this->settingsRedirect());
		} elseif ($action === 'save_topic') {
			$previousPipelineHash = $this->pipelineHash();
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
			// Same reasoning as save_settings above: startBackfill() bumps pipeline_revision, which
			// pipelineHash() hashes, so calling it unconditionally would reset the whole backlog on every save
			// regardless of whether anything pipelineHash() actually cares about changed (e.g. adjusting only
			// this topic's confidence threshold, which isn't part of the hashed topic tuple).
			if (!hash_equals($previousPipelineHash, $this->pipelineHash())) {
				$this->store()->startBackfill();
			}
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
			// Tests the saved preference, not any automatic runtime fallback, so this always reflects the
			// configuration the user is actually looking at. Each component is tested independently, and all of
			// them run concurrently (TopicDigestParallelTester), so a broken fallback (or embedding provider) is
			// never misreported as the primary being broken, and a slow/unreachable endpoint only costs its own
			// short fixed timeout rather than blocking the others or borrowing the full processing timeout.
			$timeout = $this->ollamaTimeoutConfiguration();
			$tester = new TopicDigestParallelTester();

			$primary = $this->primaryTextIdentity();
			$primaryLabel = $primary['type'] === 'openai-compatible'
				? 'OpenAI-compatible primary text provider' : "Primary text provider ({$this->storedOllamaProfile()})";
			$primaryProvider = $this->buildPrimaryTextProvider($timeout, $tester);
			$labels = [$primaryLabel];
			$operations = [static fn(): mixed => $primaryProvider->test([$primary['summary_model'], $primary['judge_model']])];

			$fallback = $this->fallbackTextIdentity();
			$fallbackProvider = $fallback === null ? null : $this->buildFallbackTextProvider($timeout, $tester);
			if ($fallback !== null && $fallbackProvider !== null) {
				$labels[] = $fallback['type'] === 'openai-compatible'
					? 'OpenAI-compatible fallback text provider' : 'Fallback text provider (local Ollama)';
				$operations[] = static fn(): mixed => $fallbackProvider->test([$fallback['summary_model'], $fallback['judge_model']]);
			}

			$embeddingModel = $this->embeddingConfiguration()['model'];
			$embeddingProvider = $this->buildEmbeddingProvider($timeout, $tester);
			$labels[] = 'Local embedding provider';
			$operations[] = static fn(): mixed => $embeddingProvider->test([$embeddingModel]);

			$outcomes = $tester->run($operations);
			$results = [];
			$ok = true;
			foreach ($outcomes as $index => $error) {
				if ($error === null) {
					$results[] = "{$labels[$index]}: OK";
				} else {
					$ok = false;
					$results[] = "{$labels[$index]}: FAILED ({$error->getMessage()})";
				}
			}

			$message = implode(' | ', $results);
			if ($ok) {
				Minz_Request::good($message);
			} else {
				Minz_Request::bad($message);
			}
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
			. ' --user ' . escapeshellarg($username)
			. ' --concurrency ' . escapeshellarg((string)$this->workerConcurrency())
			. ' >> ' . escapeshellarg($this->getExtensionUserPath() . '/worker.log')
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
