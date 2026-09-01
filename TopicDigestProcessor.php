<?php
declare(strict_types=1);

/** @phpstan-import-type TopicDigestConfig from TopicDigestExtension */
final class TopicDigestProcessor {
	private const TOPIC_DECISION_REVISION = 'topic-batch-v2';
	private const EVENT_DECISION_REVISION = 'event-batch-v1';
	/** How long to skip a primary text provider after it rejects a request for account/plan reasons (HTTP 402/429). */
	private const FALLBACK_COOLDOWN_SECONDS = 1200;
	private TopicDigestStore $store;
	/** @var TopicDigestConfig */
	private array $config;
	/**
	 * The real, always-blocking-curl text chain, used by finalize() and by the sequential (concurrency=1)
	 * path. runConcurrent() temporarily swaps this for a wavefront-scoped, dispatcher-routed chain while its
	 * prepare() fibers run, then restores it — see runConcurrent()'s doc comment for why that swap is safe.
	 */
	private TopicDigestTextProviderChain $chain;
	/**
	 * Always real-blocking curl, never dispatcher-routed, even during a concurrent wavefront: embeddings
	 * always go to Ollama, and Ollama is not what worker concurrency is meant for (see runActive()). A
	 * blocking call inside one prepare() fiber blocks the whole process until it returns, which keeps
	 * embedding calls serialized by construction rather than by a separate check.
	 */
	private TopicDigestEmbeddingProvider $embeddingProvider;
	private string $embeddingModel;
	/**
	 * How many articles' prepare() stage may run concurrently against an OpenAI-compatible endpoint; 1
	 * reproduces the original sequential worker. Has no effect while the effective text provider is Ollama
	 * (local or cloud) — see runActive()'s $effectiveConcurrency.
	 */
	private int $concurrency;

	/** @param ?int $concurrencyOverride When given (e.g. from a CLI --concurrency flag), used instead of the configured worker_concurrency. */
	public function __construct(private readonly TopicDigestExtension $extension, ?int $concurrencyOverride = null) {
		$this->store = $extension->store();
		$this->config = $extension->configuration();
		$timeout = (int)$this->config['timeout'];
		$this->chain = $extension->textProviderChain($timeout);
		$this->embeddingProvider = $extension->buildEmbeddingProvider($timeout);
		$this->embeddingModel = (string)$this->config['embedding_provider_model'];
		$this->concurrency = $concurrencyOverride !== null ? max(1, min(8, $concurrencyOverride)) : $extension->workerConcurrency();
		// Keyed on the embedding provider's own identity, never any text-provider setting: a temporary
		// text-provider fallback flips on its own every cooldown, and invalidating every embedding on each flip
		// meant they were permanently being recomputed rather than used. A genuine embedding model/endpoint change
		// is what should invalidate them, and only that, now that embeddings are fully decoupled from text
		// providers. A change may therefore leave embeddings from two models in the cache; cosine() already scores
		// mismatched dimensions as -1, so that only blurs candidate ranking.
		$embeddingIdentity = $extension->embeddingIdentityHash();
		if ($this->store->lastEmbeddingIdentity() !== $embeddingIdentity) {
			$this->store->invalidateEmbeddings();
			$this->store->setLastEmbeddingIdentity($embeddingIdentity);
		}
	}

	/** @return array{processed:int,failed:int,backfill_scanned:int,claimed:int} */
	public function run(int $limit): array {
		if ($this->store->isPaused()) {
			return ['processed' => 0, 'failed' => 0, 'backfill_scanned' => 0, 'claimed' => 0];
		}
		return $this->runActive($limit);
	}

	/** @return array{processed:int,failed:int,backfill_scanned:int,claimed:int} */
	private function runActive(int $limit): array {
		$processed = 0;
		$failed = 0;
		$scanned = 0;
		$claimed = 0;
		// Enqueue the whole remaining archive up front (not paced against the current queue depth) so
		// "queued" reflects the true amount of outstanding work, making the processing-time estimate meaningful
		// from the start instead of growing as backfill trickles more items in over time.
		if (!$this->store->isPaused()) {
			while ($this->store->backfill()['active']) {
				$count = $this->extension->enqueueBackfillPage(100);
				$scanned += $count;
				if ($count === 0 && $this->store->backfill()['active']) {
					throw new RuntimeException('Topic Digest archive scan did not advance.');
				}
			}
		}
		if ($this->store->isPaused()) {
			return ['processed' => 0, 'failed' => 0, 'backfill_scanned' => $scanned, 'claimed' => 0];
		}
		// Concurrent dispatch is only used against an OpenAI-compatible endpoint. Local and Ollama Cloud are
		// both a single Ollama server process, generally sized (per this project's own recommendations) for
		// one request at a time on modest CPU/GPU hardware — Ollama has no equivalent of an OpenAI-compatible
		// API's multi-tenant concurrent-request handling, so asking it to do several articles' inference at
		// once would add contention and queuing at best, and risks starving/OOM-ing a memory-constrained host
		// at worst, for no real throughput gain. worker_concurrency is silently clamped to 1 (fully sequential,
		// identical to the original worker) whenever the effective text provider isn't 'openai-compatible',
		// regardless of the configured value.
		$effectiveConcurrency = $this->chain->providerType() === 'openai-compatible' ? $this->concurrency : 1;
		// Bounded by jobs actually claimed, not by how many of them end up processed/failed: a job that turns
		// out stale and gets silently requeued (process()/finalize() returning false without throwing) counts
		// as one consumed attempt here, exactly as one loop iteration did in the original per-job for-loop —
		// it must not let this call claim more than $limit jobs in total, nor stop before $limit is reached
		// just because one wavefront's jobs all happened to requeue rather than complete.
		$remaining = $limit;
		while ($remaining > 0) {
			$jobs = $this->claimWavefront(min($effectiveConcurrency, $remaining));
			if ($jobs === []) {
				break;
			}
			$remaining -= count($jobs);
			$claimed += count($jobs);
			$outcome = count($jobs) === 1 ? $this->runSequential($jobs) : $this->runConcurrent($jobs);
			$processed += $outcome['processed'];
			$failed += $outcome['failed'];
		}
		return ['processed' => $processed, 'failed' => $failed, 'backfill_scanned' => $scanned, 'claimed' => $claimed];
	}

	/**
	 * Claims up to $size jobs by calling the existing (already concurrency-safe: BEGIN IMMEDIATE per call)
	 * claim() that many times in sequence, rather than adding a new bulk-claim store method — claim() is only
	 * ever called from this one, single-threaded loop, so calling it repeatedly is exactly as safe as calling
	 * it once. May return fewer than $size jobs (or none) if the queue runs out.
	 *
	 * @return list<array<string,mixed>>
	 */
	private function claimWavefront(int $size): array {
		$jobs = [];
		for ($i = 0; $i < $size; $i++) {
			$job = $this->store->claim(max(600, (int)$this->config['timeout'] * 10));
			if ($job === null) {
				break;
			}
			$jobs[] = $job;
		}
		return $jobs;
	}

	/** @param list<array<string,mixed>> $jobs @return array{processed:int,failed:int} */
	private function runSequential(array $jobs): array {
		$processed = 0;
		$failed = 0;
		foreach ($jobs as $job) {
			$jobStartedAt = hrtime(true);
			try {
				if ($this->process($job)) {
					$processed++;
					$this->recordActivity($job, $jobStartedAt);
				}
			} catch (Throwable $e) {
				if ($this->handleFailure($job, $e, $this->chain)) {
					$failed++;
				}
			}
		}
		return ['processed' => $processed, 'failed' => $failed];
	}

	/**
	 * Runs prepare() for every claimed job concurrently via a fresh TopicDigestConcurrentDispatcher (one
	 * shared curl_multi handle, one Fiber per job), then finalize()s each result one at a time in a plain
	 * sequential loop — finalize() is where event/source/feed state is read and written, and nothing else
	 * ever touches those tables, so running it strictly one job at a time (never inside a Fiber) is exactly
	 * as safe as today's fully sequential worker was.
	 *
	 * Only reached when the effective text provider is 'openai-compatible' (see runActive()'s
	 * $effectiveConcurrency): $this->chain is temporarily pointed at a wavefront-scoped, dispatcher-routed
	 * chain for the duration of the concurrent prepare() calls, then restored. This is safe despite being
	 * shared mutable state because PHP Fibers are cooperative: the reassignment itself never spans a
	 * Fiber::suspend() point, so every prepare() fiber sees the wavefront chain for its entire run, and
	 * finalize() (called only after every fiber has terminated and the original is restored) never sees it.
	 *
	 * $this->embeddingProvider is deliberately left untouched — embeddings always go to Ollama regardless of
	 * which text provider is in use, and Ollama is not what concurrency is meant for here (see runActive()).
	 * Leaving it real-blocking means an embedding call inside one prepare() fiber blocks the whole PHP process
	 * (fibers are cooperative, not real threads) until it returns, so embedding calls are never actually
	 * concurrent even during an active wavefront — only the dispatcher-routed OpenAI-compatible calls are.
	 *
	 * @param list<array<string,mixed>> $jobs @return array{processed:int,failed:int}
	 */
	private function runConcurrent(array $jobs): array {
		$timeout = (int)$this->config['timeout'];
		$dispatcher = new TopicDigestConcurrentDispatcher(min(30, max(5, $timeout)), $timeout);
		$wavefrontChain = $this->extension->textProviderChain($timeout, $dispatcher);
		$originalChain = $this->chain;
		$this->chain = $wavefrontChain;
		$startedAt = [];
		try {
			$operations = [];
			foreach ($jobs as $index => $job) {
				$startedAt[$index] = hrtime(true);
				$operations[] = fn(): array => $this->prepare($job);
			}
			$results = $dispatcher->run($operations);
		} finally {
			$this->chain = $originalChain;
		}
		$processed = 0;
		$failed = 0;
		$quotaExceeded = false;
		foreach ($results as $index => $outcome) {
			$job = $jobs[$index];
			if ($outcome['error'] !== null) {
				if ($outcome['error'] instanceof OllamaQuotaExceededException) {
					$quotaExceeded = true;
				}
				if ($this->handleFailure($job, $outcome['error'], $wavefrontChain, logCooldown: false)) {
					$failed++;
				}
				continue;
			}
			try {
				if ($this->finalize($outcome['value'])) {
					$processed++;
					$this->recordActivity($job, $startedAt[$index]);
				}
			} catch (Throwable $e) {
				if ($e instanceof OllamaQuotaExceededException) {
					$quotaExceeded = true;
				}
				if ($this->handleFailure($job, $e, $wavefrontChain, logCooldown: false)) {
					$failed++;
				}
			}
		}
		// One cooldown write for the whole wavefront rather than one per quota-exceeded article: the underlying
		// meta write is an idempotent last-write-wins upsert either way, so this is a cleanliness measure, not a
		// correctness requirement. Checked against the wavefront's own chain, which is the one that actually
		// made the failing calls.
		if ($quotaExceeded && $wavefrontChain->hasFallback() && !$wavefrontChain->usesFallback()) {
			$this->extension->markPrimaryTextUnavailable(time() + self::FALLBACK_COOLDOWN_SECONDS);
			Minz_Log::error('Topic Digest: the primary text provider hit an account/plan limit during a concurrent '
				. 'wavefront, falling back for ' . self::FALLBACK_COOLDOWN_SECONDS . ' seconds.');
		}
		return ['processed' => $processed, 'failed' => $failed];
	}

	/** @param array<string,mixed> $job */
	private function recordActivity(array $job, int $jobStartedAt): void {
		try {
			if (!$this->store->recordProcessingActivity((string)$job['entry_id'],
					(hrtime(true) - $jobStartedAt) / 1_000_000_000)) {
				Minz_Log::error('Topic Digest could not attach processing metrics to the completed job.');
			}
		} catch (Throwable $e) {
			Minz_Log::error('Topic Digest processing metrics error: ' . $e->getMessage());
		}
	}

	/**
	 * @param array<string,mixed> $job
	 * Only the primary hitting a quota/rate limit starts a cooldown: this mirrors the exact timing of the
	 * original Ollama Cloud->local mechanism (no mid-job retry through the fallback — the *next*
	 * TopicDigestProcessor construction, or in the concurrent case the rest of this wavefront's finalize loop,
	 * is what starts using it, once the cooldown is recorded). $logCooldown lets runConcurrent() suppress the
	 * per-job cooldown write and do it once for the whole wavefront instead.
	 */
	private function handleFailure(array $job, Throwable $e, TopicDigestTextProviderChain $chain, bool $logCooldown = true): bool {
		if ($logCooldown && $e instanceof OllamaQuotaExceededException && $chain->hasFallback() && !$chain->usesFallback()) {
			$this->extension->markPrimaryTextUnavailable(time() + self::FALLBACK_COOLDOWN_SECONDS);
			Minz_Log::error('Topic Digest: the primary text provider hit an account/plan limit, falling back '
				. 'for ' . self::FALLBACK_COOLDOWN_SECONDS . ' seconds: ' . $e->getMessage());
		}
		if ($this->store->failCurrent($job, $e->getMessage())) {
			Minz_Log::error('Topic Digest worker error: ' . $e->getMessage());
			$this->releaseRebuildRestoreIfAbandoned($job);
			return true;
		}
		return false;
	}

	/**
	 * A job that has used up its retries never reaches finishRebuildForEntry(), so an article a restart marked for
	 * restoration would stay read indefinitely with nothing left to un-read it. Treat exhaustion as "no longer
	 * matches" for restore purposes; an explicit Retry re-queues the job and it can still match again afterwards.
	 *
	 * @param array<string,mixed> $job
	 */
	private function releaseRebuildRestoreIfAbandoned(array $job): void {
		$entryId = (string)$job['entry_id'];
		try {
			if (!$this->store->isPendingRebuildRestore($entryId) || !$this->store->hasExhaustedRetries($entryId)) {
				return;
			}
			$entry = FreshRSS_Factory::createEntryDao()->searchById($entryId);
			if ($entry === null) {
				$this->store->completeRebuildRestore($entryId);
				return;
			}
			$this->extension->finishRebuildForEntry($entry, false);
		} catch (Throwable $e) {
			Minz_Log::error('Topic Digest rebuild-restore cleanup error: ' . $e->getMessage());
		}
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
			$summary = $this->chain->provider()->summarise($this->chain->summaryModel(),
				htmlspecialchars_decode($entry->title(), ENT_QUOTES), $text, $entry->date(true));
			$decision = $this->chain->provider()->matchTopic($this->chain->judgeModel(), $summary, $topic);
			$results[] = ['title' => htmlspecialchars_decode($entry->title(), ENT_QUOTES),
				'matches' => $decision['matches'] && $decision['confidence'] >= (float)$topic['confidence'],
				'confidence' => $decision['confidence'], 'reason' => $decision['reason']];
		}
		return $results;
	}

	/** @param array<string,mixed> $job */
	private function process(array $job): bool {
		return $this->finalize($this->prepare($job));
	}

	/**
	 * @return array{handled:bool,result:bool,job:array<string,mixed>,entry:?FreshRSS_Entry,stored:?array<string,mixed>,
	 *     summary:?array<string,mixed>,embedding:?list<float>,candidate_topics:?list<array<string,mixed>>,
	 *     topic_decisions:?array<int,array{matches:bool,confidence:float,reason:string,event_title:string}>}
	 */
	private function handled(bool $result): array {
		return ['handled' => true, 'result' => $result, 'job' => [], 'entry' => null, 'stored' => null,
			'summary' => null, 'embedding' => null, 'candidate_topics' => null, 'topic_decisions' => null];
	}

	/**
	 * The article-local work safe to run concurrently across several claimed jobs: entry lookup/recovery, the
	 * cheap skip checks (each already terminal, via handled()), and the expensive per-article inference
	 * (summary, embedding, per-topic judge decisions). Stops right before event resolution, which reads
	 * cross-article shared state (`events`) and must never act on a value read before a network call
	 * suspended this Fiber — that part lives in finalize(), which only ever runs one job at a time.
	 *
	 * Safe to run inside a Fiber even though several of its early-exit branches write to the database
	 * (queue/source-membership bookkeeping, generated-feed sync): none of them, nor anything else in this
	 * method before the inference calls, ever makes an HTTP call, so nothing here can be interleaved with
	 * another concurrently-running Fiber's code — PHP Fibers only ever switch at a Fiber::suspend() point,
	 * which only occurs inside a provider call routed through TopicDigestConcurrentDispatcher.
	 *
	 * @param array<string,mixed> $job
	 * @return array{handled:bool,result:bool,job:array<string,mixed>,entry:?FreshRSS_Entry,stored:?array<string,mixed>,
	 *     summary:?array<string,mixed>,embedding:?list<float>,candidate_topics:?list<array<string,mixed>>,
	 *     topic_decisions:?array<int,array{matches:bool,confidence:float,reason:string,event_title:string}>}
	 */
	private function prepare(array $job): array {
		$entry = FreshRSS_Factory::createEntryDao()->searchById((string)$job['entry_id']);
		if ($entry === null) {
			$entry = $this->recoverEntryByGuid($job);
			if ($entry !== null) {
				$job['entry_id'] = $entry->id();
			}
		}
		if ($entry === null) {
			$this->store->completeRebuildRestore((string)$job['entry_id']);
			// Without a GUID there is no way to tell a genuinely deleted article from a row still keyed by the
			// provisional id FreshRSS assigned before renumbering it at commit time. Only rows queued before GUIDs
			// were recorded are in that state, and the article itself, if it still exists, is queued again under
			// its current id by the archive scan, so say so rather than reporting a deletion that may not have
			// happened.
			return $this->handled((string)($job['guid'] ?? '') === ''
				? $this->store->discardStaleCurrent($job)
				: $this->store->completeCurrent($job, 'skipped', 'Entry no longer exists.'));
		}
		if ($this->extension->isSyntheticFeed($entry->feedId())) {
			return $this->handled($this->store->completeCurrent($job, 'skipped', 'Synthetic digest entry.'));
		}
		if (!hash_equals((string)$job['content_hash'], $entry->hash())
				|| !hash_equals((string)$job['pipeline_hash'], $this->extension->pipelineHash())) {
			$feed = $entry->feed();
			// enqueue() returns false without touching the row when it is already current (another process got
			// there first). The row is then still 'processing' with our lease, so release it explicitly instead of
			// leaving it to expire — each expiry costs an attempt and four of them fail the job outright.
			if (!$this->store->enqueue($entry, $feed?->categoryId() ?? 0, $this->extension->pipelineHash(),
					archive: (bool)($job['is_archive'] ?? false))) {
				$this->store->releaseCurrent($job);
			}
			return $this->handled(false);
		}
		foreach ($this->store->detachChangedSources($entry->id(), $entry->hash()) as $topicId) {
			if ($this->store->topic($topicId) !== null) {
				$this->extension->synchroniseTopic($topicId, false, true);
			}
		}
		foreach ($this->store->topicIdsForSource($entry->id()) as $topicId) {
			$membershipTopic = $this->store->topic($topicId);
			if ($membershipTopic === null || ($membershipTopic['enabled'] && !$this->topicAccepts($membershipTopic, $job))) {
				if ($this->store->removeSourceMembership($topicId, $entry->id()) && $membershipTopic !== null) {
					$this->extension->synchroniseTopic($topicId, false, true);
				}
			}
		}
		if ($entry->isFavorite() || $this->store->isProtected($entry->id())) {
			$this->extension->finishRebuildForEntry($entry, false);
			return $this->handled($this->store->completeCurrent($job, 'skipped', 'Article is protected by the user.'));
		}
		if ($this->extension->isFilterReadEntry($entry)) {
			$this->extension->finishRebuildForEntry($entry, false);
			return $this->handled($this->store->completeCurrent($job, 'skipped', 'Marked read by a FreshRSS filter.'));
		}
		if (!$this->hasEligibleTopic($job)) {
			$this->extension->finishRebuildForEntry($entry, false);
			return $this->handled($this->store->completeCurrent($job, 'skipped', 'No active topic includes this article.'));
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
				$summary = $this->chain->provider()->summarise($this->chain->summaryModel(),
					(string)$job['title'], $text, (int)$job['published_at']);
				$embedding = $this->embeddingProvider->embed($this->embeddingModel, $this->summaryText($summary));
			}
			$feedName = htmlspecialchars_decode($entry->feed()?->name(raw: true) ?? '', ENT_QUOTES);
			if (!$this->store->saveSummaryIfCurrent($job, $this->extension->analysisHash(), $feedName,
					json_encode($summary, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR), $text, $embedding)) {
				return $this->handled(false);
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
		$candidateTopics = array_values(array_filter($this->candidateTopics($job, $embedding),
			fn(array $topic): bool => !$this->store->isRejected((int)$topic['id'], $entry->id())));
		$topicDecisions = $this->topicDecisions($job, $summary, $candidateTopics);
		if ($topicDecisions === null) {
			return $this->handled(false);
		}
		return ['handled' => false, 'result' => false, 'job' => $job, 'entry' => $entry, 'stored' => $stored,
			'summary' => $summary, 'embedding' => $embedding, 'candidate_topics' => $candidateTopics,
			'topic_decisions' => $topicDecisions];
	}

	/**
	 * Everything that reads or writes cross-article shared state — event candidates, source membership for a
	 * matched topic, generated-feed synchronization, read-marking — plus the terminal queue write. Always
	 * called strictly one job at a time (from runSequential()'s plain loop, or after every prepare() Fiber in
	 * a wavefront has already terminated in runConcurrent()), so re-reading `events`/rejections here fresh is
	 * exactly as safe as it always was in the fully sequential worker: nothing else ever writes those tables.
	 *
	 * @param array{handled:bool,result:bool,job:array<string,mixed>,entry:?FreshRSS_Entry,stored:?array<string,mixed>,
	 *     summary:?array<string,mixed>,embedding:?list<float>,candidate_topics:?list<array<string,mixed>>,
	 *     topic_decisions:?array<int,array{matches:bool,confidence:float,reason:string,event_title:string}>} $prepared
	 */
	private function finalize(array $prepared): bool {
		if ($prepared['handled']) {
			return $prepared['result'];
		}
		$job = $prepared['job'];
		$entry = $prepared['entry'];
		$stored = $prepared['stored'];
		$summary = $prepared['summary'];
		$embedding = $prepared['embedding'];
		$candidateTopics = $prepared['candidate_topics'];
		$topicDecisions = $prepared['topic_decisions'];
		$matched = false;
		foreach ($candidateTopics as $topic) {
			if ($this->store->isRejected((int)$topic['id'], $entry->id())) {
				continue;
			}
			$decision = $topicDecisions[(int)$topic['id']];
			if (!$this->jobUsesCurrentPipeline($job) || $this->store->topic((int)$topic['id']) === null) {
				return false;
			}
			if (!$decision['matches'] || $decision['confidence'] < (float)$topic['confidence']) {
				if ($this->store->removeSourceMembership((int)$topic['id'], $entry->id())) {
					$this->extension->synchroniseTopic((int)$topic['id'], false, true);
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
			if (!$this->jobUsesCurrentPipeline($job)) {
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
			if ($topic['topic_type'] === 'feed') {
				$this->extension->materialiseTopicSource((int)$topic['id'], $entry->id(), $result['new_event']);
			} elseif ($topic['topic_type'] === 'digest' || (bool)$topic['show_verification']) {
				$this->extension->synchroniseTopic((int)$topic['id'], $result['new_event']);
			}
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

	/**
	 * FreshRSS assigns an entry's final id only when committing new entries, after the id captured at
	 * enqueue time (EntryBeforeInsert/EntryBeforeAdd). So a stored entry_id can be stale from the start,
	 * especially for archive/backfill jobs and entries queued around the same time as many others. Before
	 * concluding the article is gone, try to resolve its current id via its (stable) GUID instead.
	 * @param array<string,mixed> $job
	 */
	private function recoverEntryByGuid(array $job): ?FreshRSS_Entry {
		$guid = (string)($job['guid'] ?? '');
		if ($guid === '') {
			return null;
		}
		$entryDao = FreshRSS_Factory::createEntryDao();
		$resolvedId = $entryDao->searchIdByGuid((int)$job['feed_id'], $guid);
		if ($resolvedId === null || $resolvedId === (string)$job['entry_id']) {
			return null;
		}
		$entry = $entryDao->searchById($resolvedId);
		if ($entry === null || !$this->store->rekeyJob((string)$job['entry_id'], $resolvedId)) {
			return null;
		}
		return $entry;
	}

	/** @param array<string,mixed> $job */
	private function jobUsesCurrentPipeline(array $job): bool {
		return $this->store->isCurrentJob($job)
			&& hash_equals((string)$job['pipeline_hash'], $this->extension->pipelineHash());
	}

	/** @param array<string,mixed> $job @param list<float> $embedding @return list<array<string,mixed>> */
	private function candidateTopics(array $job, array $embedding): array {
		$candidates = [];
		$entryId = (string)$job['entry_id'];
		$memberships = array_fill_keys($this->store->topicIdsForSource($entryId), true);
		// A rebuild clears "sources" up front, so topicIdsForSource() alone would miss every topic an article
		// was previously matched to; this keeps those guaranteed candidates instead of leaving their fate to
		// whichever topics happen to rank in the embedding-similarity shortlist below.
		$previouslyMatched = array_fill_keys($this->store->previousTopicIdsForRebuild($entryId), true);
		foreach ($this->store->topics(true) as $topic) {
			if (!$this->topicAccepts($topic, $job)) {
				continue;
			}
			$topicEmbedding = $this->topicEmbedding($topic);
			$id = (int)$topic['id'];
			$candidates[] = ['score' => self::cosine($embedding, $topicEmbedding), 'topic' => $topic,
				'membership' => isset($memberships[$id]) || isset($previouslyMatched[$id])];
		}
		usort($candidates, static fn(array $left, array $right): int => $right['score'] <=> $left['score']);
		$selected = array_slice($candidates, 0, 5);
		// Tracked by topic id rather than by comparing whole candidate arrays: identity comparison of nested
		// arrays is both needlessly expensive and silently wrong the moment two topics compare equal by value.
		$selectedIds = array_fill_keys(array_map(static fn(array $c): int => (int)$c['topic']['id'], $selected), true);
		foreach ($candidates as $candidate) {
			if ($candidate['membership'] && !isset($selectedIds[(int)$candidate['topic']['id']])) {
				$selected[] = $candidate;
				$selectedIds[(int)$candidate['topic']['id']] = true;
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
		$judgeModel = $this->chain->judgeModel();
		// Qualified by which provider produced it (see judgeModelIdentity()), so a fallback provider that shares a
		// bare model name with the primary never collides with it in this cache.
		$judgeRevision = $this->chain->judgeModelIdentity() . "\n" . self::TOPIC_DECISION_REVISION;
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
			$batchDecisions = $this->chain->provider()->matchTopics($judgeModel, $summary, $batch);
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
		$embedding = $this->embeddingProvider->embed($this->embeddingModel, $text);
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
		$judgeModel = $this->chain->judgeModel();
		$judgeRevision = $this->chain->judgeModelIdentity() . "\n" . self::EVENT_DECISION_REVISION;
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
			$batchDecisions = $this->chain->provider()->sameEvents($judgeModel, $summary, $batch);
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
				(string)$job['entry_id'], (string)$job['content_hash'], $this->chain->summaryModel(), $this->embeddingModel
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
				(string)$job['entry_id'], (string)$job['content_hash'], $this->chain->summaryModel(),
				$this->embeddingModel, $this->summaryText($summary), $sourceText, $embedding,
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
