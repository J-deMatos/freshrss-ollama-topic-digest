<?php
declare(strict_types=1);

final class TopicDigestCloudConcurrency {
	public const INITIAL = 2;
	public const MINIMUM = 1;
	public const MAXIMUM = 16;
	private const SUCCESS_WINDOW_MULTIPLIER = 4;

	/**
	 * @param array{target:int,successes:int,cooldown_until:int,backoff_level:int,last_status:int} $state
	 */
	public function __construct(private array $state, private readonly int $maximum = self::MAXIMUM) {
		$this->state['target'] = max(self::MINIMUM, min($this->cap(), $this->state['target']));
	}

	public function target(int $now): int {
		return $this->state['cooldown_until'] > $now ? 0 : $this->state['target'];
	}

	public function cooldownUntil(): int {
		return $this->state['cooldown_until'];
	}

	public function success(): void {
		$this->state['successes']++;
		$this->state['last_status'] = 0;
		if ($this->state['successes'] >= max(4, $this->state['target'] * self::SUCCESS_WINDOW_MULTIPLIER)) {
			$this->state['target'] = min($this->cap(), $this->state['target'] + 1);
			$this->state['successes'] = 0;
			$this->state['backoff_level'] = max(0, $this->state['backoff_level'] - 1);
		}
	}

	public function throttle(int $status, ?int $retryAfter, int $now, ?int $jitter = null,
		?int $requestConcurrency = null): int {
		$previousTarget = $this->state['target'];
		$requestConcurrency ??= $previousTarget;
		if ($status === 429 || $status === 503) {
			$this->state['target'] = max(self::MINIMUM, intdiv(max(2, $previousTarget), 2));
		}
		$this->state['successes'] = 0;
		$this->state['backoff_level'] = min(10, $this->state['backoff_level'] + 1);
		$this->state['last_status'] = $status;

		if ($retryAfter !== null) {
			$delay = max(1, min(21600, $retryAfter));
		} else {
			$base = match ($status) {
				429 => min(3600, 30 * (2 ** min(7, $this->state['backoff_level'] - 1))),
				503 => min(900, 15 * (2 ** min(6, $this->state['backoff_level'] - 1))),
				default => min(120, 5 * (2 ** min(4, $this->state['backoff_level'] - 1))),
			};
			$delay = $base + ($jitter ?? random_int(0, min(30, max(1, intdiv($base, 4)))));
		}
		if ($status === 429 && $requestConcurrency === self::MINIMUM && $this->state['backoff_level'] >= 2) {
			$delay = max($delay, min(21600, 900 * (2 ** min(4, $this->state['backoff_level'] - 2))));
		}
		$this->state['cooldown_until'] = max($this->state['cooldown_until'], $now + $delay);
		return $delay;
	}

	/** @return array{target:int,successes:int,cooldown_until:int,backoff_level:int,last_status:int} */
	public function state(): array {
		return $this->state;
	}

	private function cap(): int {
		return max(self::MINIMUM, min(self::MAXIMUM, $this->maximum));
	}
}
