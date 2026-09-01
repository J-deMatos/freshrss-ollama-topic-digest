<?php
declare(strict_types=1);

require_once __DIR__ . '/TopicDigestTextProvider.php';

/**
 * Resolves which configured text provider (primary or fallback) is effective for one processing batch.
 *
 * This mirrors the existing Ollama Cloud->local cooldown mechanism exactly: the choice is made once
 * (by the caller, from TopicDigestStore's cooldown state, exactly as extension.php's
 * effectiveOllamaProfile() already does) and held for the whole batch — there is no retry-mid-call from
 * primary to fallback. A quota-exceeded response from the primary fails/retries that one job as before;
 * the *next* batch (the next TopicDigestProcessor construction) is what actually starts using the
 * fallback, once the caller has recorded the cooldown. Keeping that timing identical is deliberate: it's
 * the one already proven not to churn the queue.
 */
final class TopicDigestTextProviderChain {
	public function __construct(
		private readonly TopicDigestTextProvider $primary,
		private readonly string $primarySummaryModel,
		private readonly string $primaryJudgeModel,
		private readonly string $primaryLabel,
		private readonly ?TopicDigestTextProvider $fallback,
		private readonly ?string $fallbackSummaryModel,
		private readonly ?string $fallbackJudgeModel,
		private readonly ?string $fallbackLabel,
		private readonly bool $primaryInCooldown,
	) {
		if ($this->fallback !== null
				&& ($this->fallbackSummaryModel === null || $this->fallbackJudgeModel === null || $this->fallbackLabel === null)) {
			throw new InvalidArgumentException('A fallback text provider requires its summary model, judge model, and label.');
		}
	}

	/** A fallback is configured at all, whether or not it's the one currently effective. */
	public function hasFallback(): bool {
		return $this->fallback !== null;
	}

	/** Whether the fallback, rather than the primary, is effective for this batch. */
	public function usesFallback(): bool {
		return $this->primaryInCooldown && $this->fallback !== null;
	}

	public function provider(): TopicDigestTextProvider {
		return $this->usesFallback() ? $this->fallback : $this->primary;
	}

	public function summaryModel(): string {
		return $this->usesFallback() ? $this->fallbackSummaryModel : $this->primarySummaryModel;
	}

	public function judgeModel(): string {
		return $this->usesFallback() ? $this->fallbackJudgeModel : $this->primaryJudgeModel;
	}

	/**
	 * The effective provider's type ('ollama' or 'openai-compatible'), i.e. whichever of primary/fallback
	 * provider() currently resolves to. Used to decide whether worker concurrency should actually dispatch
	 * several articles' calls at once: a local (or cloud) Ollama endpoint is a single, often
	 * resource-constrained process not designed to be asked to do several inference calls at once the way a
	 * multi-tenant OpenAI-compatible API is, so concurrent dispatch is only used when this is 'openai-compatible'.
	 */
	public function providerType(): string {
		return $this->usesFallback() ? $this->fallbackLabel : $this->primaryLabel;
	}

	/**
	 * A judge-model cache key qualified by which provider produced it, so a fallback provider that
	 * happens to share a bare model name with the primary (or a previous fallback) never collides with
	 * it in TopicDigestStore's topic/event decision cache.
	 */
	public function judgeModelIdentity(): string {
		$label = $this->usesFallback() ? $this->fallbackLabel : $this->primaryLabel;
		return "{$label}|{$this->judgeModel()}";
	}
}
