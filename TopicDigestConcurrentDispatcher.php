<?php
declare(strict_types=1);

/**
 * Drives several TopicDigestProcessor::prepare() calls concurrently using PHP Fibers to suspend each
 * provider call at the point it would normally block on curl_exec(), and a single shared curl_multi
 * handle to drive every pending request together — the same mechanism TopicDigestParallelTester already
 * uses for the settings-page "Test connection" feature.
 *
 * Deliberately a separate class rather than a parameterized/shared TopicDigestParallelTester: that class
 * has no dedicated test coverage today and backs a real user-facing feature, so duplicating its proven
 * Fiber+curl_multi mechanics into a new, independently-testable class is lower risk than modifying it —
 * consistent with this project's existing precedent of duplicating TopicDigestOllama-shaped logic into
 * TopicDigestOpenAICompatible rather than extracting shared code that can't be regression-tested here.
 *
 * Two differences from TopicDigestParallelTester, beyond the class split: (1) configurable connect/total
 * timeouts, sourced from the real configured processing timeout rather than a short fixed connectivity-
 * check value; (2) run() captures a return value per operation, not just success/failure, since a prepare()
 * call needs to hand back its result, not merely report whether it threw. Because a transport-routed
 * TopicDigestOllama/TopicDigestOpenAICompatible call skips that class's own request() error-classification
 * entirely (it returns whatever the injected transport closure returns or throws), the transport closures
 * below must replicate that classification themselves — including turning an HTTP 402/429 (or, for
 * OpenAI-compatible, TopicDigestOpenAICompatible::looksLikeQuotaExhausted()'s heuristic) into an
 * OllamaQuotaExceededException, exactly as the real blocking request() path does, so quota-driven fallback
 * keeps working when concurrency is enabled.
 */
final class TopicDigestConcurrentDispatcher implements TopicDigestTransportSource {
	private const MAX_RESPONSE_BYTES = 2_000_000;
	private \CurlMultiHandle $multi;
	/** @var array<int,array{fiber:Fiber,handle:\CurlHandle}> Keyed by curl handle id. */
	private array $active = [];

	public function __construct(private readonly int $connectTimeoutSeconds, private readonly int $totalTimeoutSeconds) {
		$this->multi = curl_multi_init();
	}

	/** @return Closure(string,string,array<string,mixed>|null):array<string,mixed> */
	public function ollamaTransport(string $baseUrl): Closure {
		return function (string $method, string $path, ?array $payload) use ($baseUrl): array {
			[$status, $error, $body] = $this->dispatch($method, $baseUrl . $path, $payload, []);
			if ($status < 200 || $status >= 300 || $error !== '') {
				$message = $this->requestFailedMessage('Ollama', $method, $baseUrl . $path, $payload, $status, $error, $body);
				if ($status === 402 || $status === 429) {
					throw new OllamaQuotaExceededException($message);
				}
				throw new RuntimeException($message);
			}
			return $this->decodeJson($body, 'Ollama');
		};
	}

	/** @return Closure(string,string,array<string,mixed>|null,array<string,string>):array<string,mixed> */
	public function openAiTransport(): Closure {
		return function (string $method, string $url, ?array $payload, array $headers): array {
			[$status, $error, $body] = $this->dispatch($method, $url, $payload, $headers);
			if ($status < 200 || $status >= 300 || $error !== '') {
				$message = $this->requestFailedMessage('OpenAI-compatible', $method, $url, $payload, $status, $error, $body);
				if ($status === 402 || $status === 429 || TopicDigestOpenAICompatible::looksLikeQuotaExhausted($status, $body)) {
					throw new OllamaQuotaExceededException($message);
				}
				throw new RuntimeException($message);
			}
			return $this->decodeJson($body, 'OpenAI-compatible');
		};
	}

	/**
	 * Registers one request with the shared curl_multi handle and suspends the current Fiber until run()'s
	 * pump() reports it finished. Must only be called from inside a Fiber started by run() below.
	 *
	 * @param array<string,mixed>|null $payload
	 * @param array<string,string> $headers
	 * @return array{0:int,1:string,2:string} [http status, curl error, response body]
	 */
	private function dispatch(string $method, string $url, ?array $payload, array $headers): array {
		$fiber = Fiber::getCurrent();
		if ($fiber === null) {
			throw new RuntimeException('The concurrent-inference transport must run inside run().');
		}
		$body = '';
		$handle = curl_init($url);
		if ($handle === false) {
			throw new RuntimeException('Cannot initialise the concurrent inference request.');
		}
		$httpHeaders = ['Accept: application/json', 'Content-Type: application/json'];
		foreach ($headers as $name => $value) {
			$httpHeaders[] = "{$name}: {$value}";
		}
		$options = [
			CURLOPT_CUSTOMREQUEST => $method, CURLOPT_RETURNTRANSFER => false,
			CURLOPT_CONNECTTIMEOUT => $this->connectTimeoutSeconds, CURLOPT_TIMEOUT => $this->totalTimeoutSeconds,
			CURLOPT_HTTPHEADER => $httpHeaders,
			CURLOPT_WRITEFUNCTION => static function ($curl, string $chunk) use (&$body): int {
				if (strlen($body) + strlen($chunk) > self::MAX_RESPONSE_BYTES) {
					return 0;
				}
				$body .= $chunk;
				return strlen($chunk);
			},
		];
		if ($payload !== null) {
			$options[CURLOPT_POSTFIELDS] = json_encode($payload, JSON_THROW_ON_ERROR);
		}
		curl_setopt_array($handle, $options);
		curl_multi_add_handle($this->multi, $handle);
		$this->active[(int)$handle] = ['fiber' => $fiber, 'handle' => $handle];
		/** @var array{0:int,1:string} $outcome [http status, curl error] */
		$outcome = Fiber::suspend();
		[$status, $error] = $outcome;
		return [$status, $error, $body];
	}

	/** @param array<string,mixed>|null $payload */
	private function requestFailedMessage(string $provider, string $method, string $url, ?array $payload,
			int $status, string $error, string $body): string {
		$detail = [];
		if ($status > 0) {
			$detail[] = "HTTP {$status}";
		}
		if ($error !== '') {
			$detail[] = "curl error: {$error}";
		}
		if ($body !== '') {
			$detail[] = 'response: ' . $this->contentSnippet($body);
		}
		$model = is_array($payload) && is_string($payload['model'] ?? null) ? " model={$payload['model']}" : '';
		return "{$provider} request failed: {$method} {$url}{$model}" . ($detail === [] ? '.' : ' (' . implode('; ', $detail) . ').');
	}

	/** @return array<string,mixed> */
	private function decodeJson(string $body, string $provider): array {
		try {
			$result = json_decode($body, true, flags: JSON_THROW_ON_ERROR);
		} catch (JsonException $e) {
			throw new RuntimeException("{$provider} provider returned invalid JSON.", previous: $e);
		}
		if (!is_array($result)) {
			throw new RuntimeException("{$provider} provider returned invalid JSON.");
		}
		return $result;
	}

	private function contentSnippet(string $content): string {
		$normalised = trim((string)preg_replace('/\s+/', ' ', $content));
		if ($normalised === '') {
			return '(empty)';
		}
		return mb_strlen($normalised, 'UTF-8') > 400 ? mb_substr($normalised, 0, 400, 'UTF-8') . '…' : $normalised;
	}

	/**
	 * Runs each operation concurrently. Each is expected to internally call (only) a transport() closure
	 * obtained from this same instance, any number of times (including zero).
	 *
	 * @template T
	 * @param list<Closure(): T> $operations
	 * @return list<array{value:?T,error:?Throwable}> One entry per operation, in the same order. Exactly one
	 *     of value/error is non-null on success/failure respectively; a null-returning operation and a
	 *     successful-but-null result are indistinguishable, which none of this dispatcher's callers rely on.
	 */
	public function run(array $operations): array {
		$results = array_fill(0, count($operations), null);
		$pending = [];
		foreach ($operations as $index => $operation) {
			$fiber = new Fiber(static function () use ($operation, $index, &$results): void {
				try {
					$results[$index] = ['value' => $operation(), 'error' => null];
				} catch (Throwable $e) {
					$results[$index] = ['value' => null, 'error' => $e];
				}
			});
			$pending[] = $fiber;
			$fiber->start();
		}
		$this->pump();
		foreach ($pending as $index => $fiber) {
			if (!$fiber->isTerminated()) {
				// Only reachable if pump() returned with work still outstanding, which it does not unless a
				// curl handle vanished from $this->active without ever finishing.
				$results[$index] = ['value' => null, 'error' => new RuntimeException('Concurrent inference task did not complete.')];
			}
		}
		return $results;
	}

	private function pump(): void {
		if ($this->active === []) {
			return;
		}
		do {
			do {
				$status = curl_multi_exec($this->multi, $running);
			} while ($status === CURLM_CALL_MULTI_PERFORM);
			if ($running > 0) {
				curl_multi_select($this->multi, 1.0);
			}
			while (($info = curl_multi_info_read($this->multi)) !== false) {
				$handle = $info['handle'];
				$id = (int)$handle;
				$entry = $this->active[$id] ?? null;
				curl_multi_remove_handle($this->multi, $handle);
				if ($entry === null) {
					curl_close($handle);
					continue;
				}
				unset($this->active[$id]);
				$httpStatus = (int)curl_getinfo($handle, CURLINFO_HTTP_CODE);
				$error = curl_error($handle);
				curl_close($handle);
				$fiber = $entry['fiber'];
				if ($fiber->isSuspended()) {
					// The resumed transport() call reads the response body via its own by-reference local
					// variable (already fully populated by CURLOPT_WRITEFUNCTION); only status/error cross here.
					$fiber->resume([$httpStatus, $error]);
				}
			}
		} while ($running > 0 || $this->active !== []);
	}
}
